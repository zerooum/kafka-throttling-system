## Throttle Test Console

A dependency-free browser console for exercising the system is served by Quarkus
at `http://localhost:8080/` when the app is running.

- **Send 1** — publish a single message (fresh idempotency key).
- **Resend last key** — re-send the previous message with the same key to trigger
  a `409` duplicate.
- **Send burst** — flood N messages (default 500) to push past the rate limit.
- The dashboard polls `/q/metrics` and `/admin/*` every second to show published /
  accepted / duplicate / consumed / backlog tallies, the circuit-breaker state, a
  live consumption-rate chart (with the limiter ceiling drawn as a red dashed
  line), and the token-bucket gauge.

Admin endpoints need the token from `admin.token` (default `dev-admin`); set it in
the Admin token field. Open `http://localhost:8080/tests.html` to run the parser /
chart self-tests.
