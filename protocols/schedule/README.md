# protocols/schedule

The `schedule` source emits a message when a cron expression matches, or every interval. With a route, transforms and a destination, that is a scheduled job, with no other tool next to the relay.

```yaml
sources:
  - name: morning-report
    type: schedule
    cron: "0 8 * * 1-5"          # every weekday at 08:00
    timezone: Europe/Madrid       # default UTC
    body: '{"report": "daily"}'  # default {}

  - name: heartbeat
    type: schedule
    interval: 30s

routes:
  - name: report
    from: [morning-report]
    transforms:
      - enrich: { url: "https://stats.internal/daily", into: stats }
    to: [slack]
```

It is in `hikyaku` and in `hikyaku-lite`.

## Keys

| Key | Meaning |
|---|---|
| `cron` | When to emit a message: a five-field cron expression, or a macro. Set `cron` or `interval`. |
| `timezone` | The time zone `cron` is read in, such as `Europe/Madrid`. Default `UTC`. The zone names are built in, so a container without zone files works. |
| `interval` | Emit a message every this long, counted from when the relay started. At least `1s`. |
| `body` | The body of each message. Default `{}`. A body that is JSON gets the content type `application/json`, any other `text/plain`. |
| `headers` | Headers set on each message. |

## Cron expressions

The five fields are minute, hour, day of month, month and day of week.

| Field | Values |
|---|---|
| minute | `0`-`59` |
| hour | `0`-`23` |
| day of month | `1`-`31` |
| month | `1`-`12`, or `jan`-`dec` |
| day of week | `0`-`7` (`0` and `7` are Sunday), or `sun`-`sat` |

Each field takes `*`, a number, a range (`1-5`), a list (`1,15`) and a step (`*/10`, `8-18/2`). When both the day of month and the day of week are set, a day that matches either one matches, as in the cron of Unix.

| Macro | Same as |
|---|---|
| `@hourly` | `0 * * * *` |
| `@daily`, `@midnight` | `0 0 * * *` |
| `@weekly` | `0 0 * * 0` |
| `@monthly` | `0 0 1 * *` |
| `@yearly`, `@annually` | `0 0 1 1 *` |

There is no seconds field. Use `interval` for something more often than every minute. `hikyaku validate` refuses an expression that is wrong, or that never matches (`0 0 30 2 *`).

When the clocks change: a time that the clock skips (02:30 on the night the clocks go forward) does not run that day. A time that comes twice runs once. An expression with `*` for the hour runs through both hours.

## The message

| Field | Value |
|---|---|
| ID | `<source name>@<the tick in UTC, RFC 3339>`, such as `morning-report@2026-11-16T07:00:00Z` |
| body, content type, headers | from `body` and `headers` |
| `schedule.time` attribute | the time the message was scheduled for, in the schedule's time zone |
| `schedule.cron` or `schedule.interval` attribute | the schedule that made it |

The body is fixed. To build a body from the time, use a route transform: `{{ index .attributes "schedule.time" }}` is the tick.

## What a schedule does not do

- **It does not make up for a missed tick.** A tick that passes while the relay is down, or while the relay cannot take the message, is skipped. The next message is the next tick.
- **It does not pick one replica.** Every replica of a relay emits its own message. With `cron`, the replicas emit the same ID for the same tick, so a destination that is `idempotent` (it dedupes by idempotency key) takes the message once. With `interval`, the ticks depend on when each replica started, so the IDs differ. Run a scheduled job on one replica, or send it to a destination that dedupes.
- **It is as exact as the machine's clock.** The source looks at the clock at least once a minute, so a suspended machine or a corrected clock delays a tick by a minute at most.

`dedupe`, `auth` and `ack` do not apply. A message whose delivery fails follows the destination's retries, fallbacks and the dead-letter queue, like any other.

## Health

A schedule source is always healthy: it depends on nothing outside the relay.
