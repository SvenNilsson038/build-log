# Enqueue background jobs from an API request: Postgres queue, worker, idempotency key

**Short answer:** write the job row into Postgres inside the same transaction as the business write that justifies it, dedupe on a client-supplied idempotency key backed by a unique index, and let a separate worker process claim rows with `SELECT ... FOR UPDATE SKIP LOCKED` under an explicit lease. A broker doesn't fix duplicate deliveries; a transactional enqueue plus an idempotent consumer does.

I run the cron and queue tier for a payments-adjacent product. Two pages in the last year came from this exact seam between an HTTP handler and a worker, and neither of them was a throughput problem.

Your API is Express. Mine is Go sitting behind a thin Node edge, so I'll show the worker in Go — that's the code I actually get paged on. The SQL transfers verbatim, and everything below pastes into `pg` without changes.

## The invariant: the enqueue commits with the write that caused it

The failure I keep seeing in review is the dual write. Handler opens a transaction, inserts an order, commits, then publishes to the broker. Looks fine. It isn't: the process can die between `COMMIT` and `publish`, and now you have an order nobody will ever fulfil, with no error anywhere and no row to alert on.

Flip it and you get the mirror bug. Publish first, commit second, and the consumer starts working a job whose row doesn't exist yet — it reads a stale snapshot, decides the record is missing, and either crashes or silently drops the work. I've watched a consumer do that at a 3% rate under normal load, which is exactly the rate that's too low to notice and too high to survive an audit.

The invariant that came out of the second postmortem is one line, and it's the only architectural rule in this article: a job is durable if and only if it is part of the transaction whose effects it depends on. If the order commits, the job exists. If the order rolls back, the job never happened. There is no window.

That rule is what makes a Postgres-backed queue attractive for the first few years of a product's life. Your jobs table lives in the same database as your business data, so the enqueue is just another `INSERT` in a transaction you already had open. No outbox relay, no two-phase commit, no reconciliation cron writing apology emails at 4am.

Here's the table I've settled on after a few rewrites:

```sql
create table job (
  id           bigserial   primary key,
  kind         text        not null,
  payload      jsonb       not null,
  idem_key     text,
  state        text        not null default 'ready',
  run_after    timestamptz not null default now(),
  lease_until  timestamptz,
  attempts     int         not null default 0,
  last_error   text
);

create unique index job_idem_uniq
  on job (kind, idem_key) where idem_key is not null;

create index job_claimable
  on job (run_after) where state in ('ready', 'running');
```

The partial unique index is the whole dedup mechanism. Postgres enforces it under concurrency, across processes, across deploys, with no coordination code on my side.

## How should an API request hand work to a background worker?

The handler does three things and nothing else: validate, write the row and the job in one transaction, return `202` with a job id the caller can poll. It must not do the work, and it must not wait for the work.

The idempotency key comes from the client, in the `Idempotency-Key` header, and it's the client's job to keep it stable across retries. That convention is written up as an IETF draft and every serious payments API uses some version of it. Treat it as required for anything that spends money or sends a message, and optional for jobs that are naturally safe to run twice.

```go
// Enqueue runs inside the caller's transaction. The job and the row that
// justifies it commit together, or neither exists.
//
// A repeated Idempotency-Key returns the id of the job already recorded,
// so an HTTP retry is indistinguishable from the first attempt.
func Enqueue(ctx context.Context, tx *sql.Tx, kind, idemKey string, payload []byte, runAfter time.Time) (int64, error) {
	const q = `
insert into job (kind, payload, idem_key, run_after)
values ($1, $2, nullif($3, ''), $4)
on conflict (kind, idem_key) where idem_key is not null
do update set kind = excluded.kind
returning id`

	var id int64
	err := tx.QueryRowContext(ctx, q, kind, payload, idemKey, runAfter).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("enqueue %s: %w", kind, err)
	}
	return id, nil
}
```

The `do update set kind = excluded.kind` looks pointless and isn't. A bare `do nothing` returns zero rows on conflict, so the retry would get no id back; the no-op update makes `RETURNING` fire on both paths. It costs one dead tuple per duplicate request, which autovacuum handles without complaint at our volume.

One detail people skip: store the idempotency key on the job, not only in a separate keys table. When someone asks me at 03:00 whether a customer got charged twice, I want to answer it with one `WHERE idem_key = $1` against the same table that holds the attempt count and the last error, not by joining three places and hoping the retention windows line up.

## Claiming work: SKIP LOCKED, leases, and the config footgun that paged me

The worker loop is a poll with a lock-skipping claim. `FOR UPDATE SKIP LOCKED` lets N workers pull disjoint batches without blocking each other, which is the feature that makes this pattern viable at all — before it existed you either serialized on an advisory lock or invented your own claim column and got it subtly wrong.

```go
const claimSQL = `
update job set
  state       = 'running',
  lease_until = now() + $1::interval,
  attempts    = attempts + 1
where id in (
  select id from job
  where (state = 'ready'   and run_after   <= now())
     or (state = 'running' and lease_until <  now())
  order by run_after
  for update skip locked
  limit $2
)
returning id, kind, payload, attempts`

func (w *Worker) claim(ctx context.Context, batch int) ([]Job, error) {
	rows, err := w.db.QueryContext(ctx, claimSQL, w.lease.String(), batch)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	defer rows.Close()
	// ... scan into []Job
}

// Fail fast on config. A zero lease means every poll re-claims work that
// is still running, which is the same thing as having no queue at all.
func NewWorker(db *sql.DB) (*Worker, error) {
	raw := os.Getenv("JOB_LEASE")
	lease, err := time.ParseDuration(raw)
	if err != nil || lease < time.Second {
		return nil, fmt.Errorf("JOB_LEASE must be a duration like 30s, got %q", raw)
	}
	return &Worker{db: db, lease: lease}, nil
}
```

That constructor exists because of a specific night. We moved the lease from a compiled-in constant to an environment variable, and the deploy config said `JOB_LEASE=30` — no unit suffix. Go's `time.ParseDuration` rejects a bare number, we were swallowing the error with `lease, _ :=`, and the zero value came out as a lease that expired the instant it was written. Every poll re-claimed every in-flight job. I got paged at 02:41 on 4,812 duplicate webhook deliveries in nine minutes, and the confusing part was that the graphs looked healthy: CPU flat, queue depth flat, error rate zero. The system was doing exactly what I'd configured it to do. We shipped the validation the same night and added a startup log line that prints the parsed lease in seconds, because the number in the env file and the number the process believes are different things and only one of them matters.

Pick the lease from your p99 job duration, not your average. If it's shorter than the work, you get duplicates; if it's much longer, a crashed worker's jobs sit stalled for that whole window. Brokers expose the same knob under a different name — SQS calls it the visibility timeout, defaults it to 30 seconds and caps it at 12 hours — and the tuning trade-off is identical no matter where the queue lives.

Then the consumer has to actually be idempotent, because the lease only narrows the duplicate window, it never closes it. Every handler I own either writes with a unique constraint on the effect (a charge row keyed by the same idempotency key), or checks a completion record before calling out, or targets an API that accepts an idempotency key of its own. I'm not sure there's a fourth option that survives contact with a network partition.

## Where a Postgres queue stops being the right call

The catch is that a table is not a broker, and pretending otherwise has a ceiling. Polling churn shows up as constant `UPDATE` traffic on a hot, heavily-indexed table; every claim creates dead tuples, and autovacuum on a busy job table becomes something you tune rather than something you ignore. Around low-thousands of jobs per second I'd stop, and if a single job holds a connection for minutes you'll starve your pool long before you hit that number.

| Concern | Postgres + SKIP LOCKED | Managed queue (e.g. SQS) | Broker (e.g. RabbitMQ) |
| --- | --- | --- | --- |
| Delivery | at-least-once, lease-bounded | at-least-once (standard queues) | at-least-once with acks |
| Transactional enqueue | native, same transaction | needs an outbox | needs an outbox |
| Priority | `ORDER BY` any column | not built in | `x-max-priority`, small range advised |
| Delayed jobs | `run_after` column | per-message delay, 15 min max | plugin or dead-letter trick |
| Inspect a stuck job | `SELECT` | console or API | management UI |
| Ops cost | zero new systems | zero to run, metered | you run it |

Priority is the honest weak spot on the broker side, and it cuts both ways. RabbitMQ supports priority queues but its docs advise keeping the number of priority levels small — one to five — because each level costs resources per queue. In Postgres, priority is `ORDER BY priority DESC, run_after` and an index; changing the policy is a query change, not a topology change. Whether that flexibility is worth the vacuum tax depends on how weird your scheduling rules are.

Stick with a dedicated broker when you need fan-out to many independent consumer groups, cross-datacenter replication of the queue itself, or sustained rates where the database is already your bottleneck. Move to one early if your jobs are long-running media work rather than short API calls. And if you're on a serverless platform where every worker is a cold process with a fresh connection, the polling model fits badly enough that a push-based queue is the simpler answer.

Whatever you pick, instrument the same two numbers: age of the oldest claimable job, and the count of jobs whose `attempts` exceeded your retry budget. Queue depth is a vanity metric — it can sit at zero while a poison job retries forever. Oldest-age is what wakes me up, and it's the one graph I'd keep if I could only keep one.

## References

- Amazon SQS visibility timeout — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- RabbitMQ priority queues — https://www.rabbitmq.com/docs/priority
- PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED` — https://www.postgresql.org/docs/current/sql-select.html
- PostgreSQL `INSERT ... ON CONFLICT` — https://www.postgresql.org/docs/current/sql-insert.html
- The Idempotency-Key HTTP header field (IETF draft) — https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/
- Go `time.ParseDuration` — https://pkg.go.dev/time#ParseDuration
