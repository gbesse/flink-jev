# Flink Jev: semantic decisions in Flink SQL

Community model provider for Apache Flink 2.3 `ML_PREDICT`. It asks TypeSafe Jev one yes/no question per row and returns a probability, a route (`yes`, `no`, `review`, `failure`), and a SHA-256 digest of the text. Uncertain answers go to `review`; network failures and invalid responses go to `failure`.

## Install

Build with Java 17 and Maven 3.9.6 or newer:

```sh
mvn -B verify
```

Put `target/flink-jev-0.1.1.jar` in the `lib` directory of every JobManager and TaskManager, then restart the cluster. Set `JEV_API_KEY` in the TaskManagers' environment. Never put the key in SQL options or a Git-tracked file.

```sql
CREATE MODEL jev_review
INPUT (content STRING)
OUTPUT (probability DOUBLE, route STRING, state_sha256 STRING)
WITH (
  'provider' = 'jev',
  'question' = 'Does the review recommend the film?',
  'model' = 'jev-1.13.0',
  'threshold' = '0.8',
  'max-input-bytes' = '32768',
  'timeout-ms' = '10000',
  'max-calls-per-task' = '10000'
);

SELECT review_id, route, probability
FROM ML_PREDICT(TABLE reviews, MODEL jev_review, DESCRIPTOR(content));
```

The example assumes a `reviews` table with `review_id` and `content`. `endpoint` defaults to `https://api.typesafe.ai/v1/systemone`; HTTP is allowed only for `localhost` and `127.0.0.1` in tests.

## Example: monitor the routes

After creating the model above, the [SQL example](examples/route-monitoring.sql) groups rows by route. `review` needs a human decision and `failure` needs error handling; neither should be interpreted as `no`. The SQL assumes the `reviews` table described above and makes one remote call per eligible row. Start with a small, permitted sample.

## Behavior and limits

- Every eligible row makes a remote request. Filter upstream and cap calls per task.
- Empty or oversized input goes to `review`. Content is sent to TypeSafe; use only permitted streams.
- `failure` is distinct from `no` so outages never become negative decisions.
- The connector does not return input text; the digest helps audit decisions without copying it.
- This initial connector expects one text column and an append-only stream. Validate cost and latency against your workload.

MIT licensed. Independent community project, unaffiliated with Apache Flink or TypeSafe AI.

[Français](README.md) · [Español](README.es.md)
