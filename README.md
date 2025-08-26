# backendforautobot

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

## Authentication

The frontend dashboard URL used after a successful Upstox login can be
configured via the `FRONTEND_DASHBOARD_URL` environment variable (property
`frontend.dashboard-url`). By default this points to the deployed Vercel
frontend. The current value can be retrieved from the backend at
`GET /auth/redirect-url`.

For CORS, adjust the `cors.allowedOrigins` property if you need to allow
additional origins. By default it allows the deployed Vercel domain and
`http://localhost:4200` for local development.
