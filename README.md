# Agile Pulse (Go/Gin starter)

LLM-powered agile coach for Jira. Converts raw activity into actionable signals, tracks flow metrics, and sends Telegram digests.

## Quickstart
```bash
cp .env.example .env
docker compose up --build -d
# DB schema is applied automatically on first DB start.
curl http://localhost:8080/healthz
curl -X POST http://localhost:8080/admin/run
```

### Railway deployment (quick)

- Set environment variables in Railway (copy from `.env.example`). Ensure:
  - `DB_DSN` points to your Railway Postgres
  - `PUBLIC_BASE_URL` is your Railway HTTPS URL
  - `TELEGRAM_*` configured; optionally set `OPENAI_API_KEY`
- Build using this Dockerfile (Railway auto-detects).
- Expose port `8080`.
- Optional: set `CRON_SPEC` to your schedule (default: Fridays 10:00 Asia/Tehran).

### Telegram webhook
Set `PUBLIC_BASE_URL` and the app will compute the webhook URL.

Example (production):
```bash
export PUBLIC_BASE_URL=https://your-prod-domain.example
export TELEGRAM_WEBHOOK_SECRET=$(openssl rand -hex 24)
export TELEGRAM_BOT_TOKEN=xxx
curl -X POST "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/setWebhook" \
  -d "url=${PUBLIC_BASE_URL}/telegram/webhook/${TELEGRAM_WEBHOOK_SECRET}" \
  -d "secret_token=${TELEGRAM_WEBHOOK_SECRET}" \
  -d "drop_pending_updates=true" \
  -d "allowed_updates[]=message" \
  -d "allowed_updates[]=callback_query"
```

Local dev defaults to `PUBLIC_BASE_URL=http://localhost:8080`.

## Layout
- `cmd/api` — entrypoint
- `internal/config, logger, http, jobs, domain, repo, adapters, services`

## Config

Environment variables (see `.env.example`):

- PUBLIC_BASE_URL: external https base url (for Telegram webhook auto-register)
- DB_DSN: postgres DSN
- JIRA_*: base URL, credentials (PAT or basic), API version (2), board names, expedite JQL, fields file
- TELEGRAM_*: bot token, webhook secret, chat IDs or usernames
- APP_TZ: timezone, CRON_SPEC: schedule
- OPENAI_*: API key, model (o3-mini/gpt-4.1-mini), timeout
- WORKERS_* and LLM_*: concurrency and token/issue budgets

Custom fields mapping: mount `config/jira_fields.json` and set `JIRA_FIELDS_FILE`.

## Jira

- Server/DC v2 endpoints used for comments and changelog. History is fetched via `issue?expand=changelog` and paginated via `/changelog` when needed.

## Anomaly Prefilter (G)

- Computes a risk score using recency, priority, type (bug), and activity density (comments+history), then selects top issues respecting `LLM_MAX_ISSUES` and `LLM_TOKEN_BUDGET`.

## LLM (I)

- Extraction and summary are implemented using OpenAI Chat Completions with the configured model. Provide `OPENAI_API_KEY` to enable.

### Configuring Jira custom fields
Optionally provide a mapping file to stabilize field IDs across Jira instances. Create `config/jira_fields.json` (or mount one at `/config/jira_fields.json`) with an array of `{ id, name }` entries, e.g.:

```json
[
  {"id":"customfield_10110","name":"Story Points"},
  {"id":"customfield_10109","name":"Epic Link"},
  {"id":"customfield_10112","name":"Subteam"},
  {"id":"customfield_10404","name":"Service"}
]
```

Set env `JIRA_FIELDS_FILE` if using a different path. The app will prefer these IDs when present and fall back to runtime discovery otherwise.

