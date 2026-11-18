# Delivery (M4)

The delivery service turns `alerts.pending` into email. It must not lose an alert, should not send one twice,
and must never flood an inbox or exceed Gmail's sending limits.

## A durable queue in Postgres

```
alerts.pending ──intake──▶ deliveries (pending) ──claim──▶ sending ──▶ sent
                 commit offsets                   │  └─temporary error──▶ pending (backoff)
                 after insert                     └─permanent / out of attempts──▶ failed + alerts.dlq
```

- **Intake** inserts each alert, then commits the Kafka offset. A crash in between re-inserts the same
  alert, and the insert ignores it. Two unique keys deduplicate: the alert ID (same event, same search),
  and a meaning key `search | listing | type | price` that catches the same news arriving twice through
  another path, such as a replay.
- **Why a table and not just the topic?** Delivery needs per-row state: retry times, attempt counts,
  holds for rate limits, leases, history. A Kafka consumer can only move forward through its partition,
  and one user's held alert would block everyone behind it. The topic is the hand-off; the table is the
  work queue.
- **Senders** claim due rows with `FOR UPDATE SKIP LOCKED` and a 2-minute lease, so any number of sender
  goroutines and replicas share the work without double-claiming. A per-user advisory lock inside the claim
  keeps one user's due alerts with one sender. Without it, the first live run sent each test user 4
  emails in the same second (one per sender) instead of one digest.
- **Crash mid-send.** A reaper returns expired leases to the queue, so delivery is at-least-once. The
  `Message-ID` is derived from the alert IDs, so a resend after a crash carries the same ID and Gmail drops
  the duplicate.

## One email per user, within limits

- **Coalescing:** alerts wait 5 s before sending, so one listing that matches three of a user's searches
  arrives as one email.
- **Digests:** everything due for a user goes in one email (up to 25 listings, the rest in the next one).
- **Per-user token bucket** in Redis (Lua, atomic): 5 emails right away, then 1 per 10 minutes. Alerts over
  the limit are deferred, not dropped, and arrive together in the next email. Deferring doesn't count as an
  attempt. If Redis is down, alerts are held for 30 s rather than sent unthrottled.
- **Daily cap** per channel (default 1,500, under Gmail Workspace's 2,000), spread across the day.
- **Refunds:** a send that fails gives its token back, so the limit counts emails delivered, not attempts.
  The end-to-end test found this: two failed attempts had locked a user out for an hour.

## Failures

| Failure | Handling |
| --- | --- |
| Connection refused, timeout, SMTP 4xx | retry after 1 m, 5 m, 30 m, 2 h, then 6 h (±10% jitter), up to 6 attempts |
| SMTP 5xx (bad address, rejected) | dead-lettered at once |
| 5xx that says "try again", rate or quota (Gmail uses 550 for its daily quota) | treated as temporary |
| Out of attempts | dead-lettered |
| `alerts.dlq` itself unreachable | kept retryable rather than marked failed, so nothing is lost |

`alerts.dlq` keeps 30 days of `{alert, cause, attempts, failed_at}` for inspection or replay.

The end-to-end test runs against real Postgres (in a throwaway schema), Redis and Mailpit. Mailpit's chaos
mode makes its SMTP server reply 451 or 550 on demand. The test covers: a digest; the rate limit holding one
email back; a 451 retried and delivered once the server recovers; a 550 dead-lettered without retries; and an
unreachable server retried until attempts run out.

## What an email contains

Subject like `Price drop: 123 Main Street, Orleans · $709,900`, or `3 updates for your saved searches`. The
body has a card per listing with the address, price change, beds, baths and type, a link to the listing on
the site that holds the search, the listing brokerage (an IDX/VOW display rule), and which saved search
matched.

- Sold and leased prices appear only for `vow` users. Public users get "Sign in to see the sold price"
  instead (the matcher has already removed the price).
- Addresses a brokerage withheld are shown as "Home in Convent Glen, Orleans".
- **Unsubscribe:** each card links to stopping that search, and the footer to stopping all alerts. The
  `List-Unsubscribe` and `List-Unsubscribe-Post` headers give one-click unsubscribe (RFC 8058), which Gmail
  requires of bulk senders. Links carry HMAC-signed tokens that don't expire. A GET only shows a
  confirmation page, because mail scanners open every link; the POST unsubscribes.

## Channels

`Channel` is a two-method interface (`Name`, `Send`). Email uses SMTP: Mailpit in development, so nothing
leaves the machine, and Gmail (`smtp.gmail.com:587`, STARTTLS, an app password) in production. SMS and push
are placeholders that log what they would send. A real provider is one `Channel` implementation.

## Measured (2026-11-18, local stack, Mailpit)

- Catching up on the alerts saved overnight for the five test searches: 162 alerts became 20 digest emails,
  each user within the burst limit.
- A fresh, unthrottled alert went from the poller seeing the change to Mailpit accepting the email in 5.3 s:
  the 5 s coalescing window plus sending. From the MLS modification timestamp it was 160 s, mostly the 60 s
  poll and PropTx's own delay. Alerts held by the rate limit waited 4–7 minutes, as configured.
