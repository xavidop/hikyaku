# kafka-bridge

This example moves messages both ways between HTTP and Kafka: HTTP events go into a Kafka topic, and a Kafka topic goes out to a webhook. It has two routes:

- **ingest:** `POST /events` lands on the `events.raw` topic, keyed by `customer_id`. Messages go out in batches, one produce call per batch, through the idempotent producer (the broker drops the duplicates of the producer's own internal retries). While the brokers are down, a file spool takes the messages. With the default `ack: delivered`, the caller waits while `raw` retries, then gets its answer once the spool has the message. It never waits for Kafka to come back.
- **deliver:** a consumer group reads the `orders` topic and calls the `fulfilment` webhook. `ordering: {}` delivers each partition's records one at a time and in order. The source commits an offset (tells Kafka how far it has read) only once every earlier record of its partition is delivered or dead-lettered, so a restart resumes where it stopped.

Two specs ship with the example:

- `hikyaku.yaml`: both routes, against the Redpanda (a Kafka-compatible broker) in `docker-compose.yml`.
- `hikyaku.llmfake.yaml`: the ingest route offline. A fault destination (one that fails on purpose) stands in for Kafka and is down, so the spool takes everything.

## Run it

Start Redpanda and the relay, post an event, and read it back from the topic:

```sh
docker compose up -d                       # Redpanda on localhost:19092
hikyaku run &
curl -s localhost:8080/events -d '{"customer_id":"c1","type":"signup"}'
docker compose exec redpanda rpk topic consume events.raw -n 1
```

Now the other direction: publish an order, and the webhook gets it. Point `fulfilment.url` at any endpoint that answers a POST with 2xx. For a try, an echo server does (`docker run --rm -p 9000:8080 mendhak/http-https-echo:36`). An endpoint that keeps failing gets 5 attempts. Then the record goes to the DLQ (the dead-letter queue, where a message goes when every delivery attempt failed) and its offset is committed:

```sh
echo '{"order":"o-1"}' | docker compose exec -T redpanda rpk topic produce orders
```

## Offline

Run the ingest route with Kafka down, and watch the spool take the message:

```sh
hikyaku run -f hikyaku.llmfake.yaml &
curl -s localhost:8080/events -d '{"customer_id":"c1"}'
cat out/spool.ndjson
```

`hikyaku_fallback_total{from="raw",to="spool"}` counts what the spool took.
