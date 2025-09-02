# backendforautobot

## DBI5 STANDARD FLOW
This backend implements a self-sufficient market feed. On startup it loads NSE instruments, connects to Upstox V3 and derives ATM option selections from live NIFTY future ticks.
Endpoints:
- `GET /status/market` — market online status
- `GET /ws/status` — websocket diagnostics
- `GET /ltp/nifty-fut` and `GET /ltp/options-atm` — latest prices
The flow and endpoints are stable and should be extended, not altered, in future tasks.

- Auto-refreshes weekly NIFTY options after expiry and includes NIFTY future in live feed.

## Logging

The application uses [Logback](https://logback.qos.ch/) for logging. Logs are written to
`logs/app.log` with daily rotation and a maximum size of 10 MB per file.

To forward logs to an external service such as Papertrail, Logtail or Graylog,
configure the following environment variables:

| Variable | Description |
|----------|-------------|
| `LOG_AGGREGATOR_HOST` | Hostname of the log collector. |
| `LOG_AGGREGATOR_PORT` | TCP port of the collector. |
| `LOG_AGGREGATOR_TOKEN` | Optional API key/token if required by your provider. |

You can also set `LOG_DIR` to change the directory where local log files are
stored (defaults to `logs`).

## WebSocket diagnostics

Set the environment variable `WS_DEBUG=true` to enable verbose TLS and Netty
wire logging for the Upstox WebSocket client. When enabled, run the JVM with
`-Djavax.net.debug=ssl,handshake` for detailed handshake traces.

On Railway, add `WS_DEBUG` in the service's environment variable settings.
If your base image lacks CA certificates, install the `ca-certificates`
package (Alpine) or use a JRE image that includes a populated `cacerts`
trust store.

## Authentication

The frontend dashboard URL used after a successful Upstox login can be
configured via the `FRONTEND_DASHBOARD_URL` environment variable (property
`frontend.dashboard-url`). By default this points to the deployed Vercel
frontend. The current value can be retrieved from the backend at
`GET /auth/redirect-url`.

For CORS, adjust the `cors.allowedOrigins` property if you need to allow
additional origins. By default it allows the deployed Vercel domain and
`http://localhost:4200` for local development.

## DBI5 Signal Engine

The backend aggregates the top-five bid and ask levels to compute a Depth
Bid-Ask Imbalance (DBI5) value. When this imbalance persists for 800 ms and
exceeds configured thresholds, a `BUY_CE` or `BUY_PE` signal is emitted with
suggested stop-loss and take-profit prices.

### Streaming signals

Signals are exposed as Server Sent Events:

```bash
curl http://localhost:8080/signals/live
```

Each event payload has the schema:

```json
{"ts":"2025-09-01T19:19:58.252806Z","symbol":"TEST","side":"BUY_CE","dbi":2.0,"sl":85.0,"tp":125.0}
```
