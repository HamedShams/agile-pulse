-- Initial schema (auto-applied on first DB start via docker-compose volume mapping)
CREATE TABLE IF NOT EXISTS issues(
  id BIGSERIAL PRIMARY KEY,
  key TEXT UNIQUE NOT NULL,
  project TEXT, type TEXT, priority TEXT,
  assignee TEXT, reporter TEXT, status_last TEXT,
  created_at_jira TIMESTAMPTZ, updated_at_jira TIMESTAMPTZ, done_at TIMESTAMPTZ,
  points_estimate NUMERIC, squad TEXT,
  due_at TIMESTAMPTZ,
  labels TEXT[],
  subteam TEXT,
  service TEXT,
  epic_key TEXT
);

CREATE TABLE IF NOT EXISTS events(
  id BIGSERIAL PRIMARY KEY,
  issue_id BIGINT REFERENCES issues(id) ON DELETE CASCADE,
  kind TEXT,
  field TEXT,
  from_val TEXT,
  to_val TEXT,
  at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_events_issue_at ON events(issue_id, at);
CREATE UNIQUE INDEX IF NOT EXISTS ux_events_unique ON events(issue_id, field, from_val, to_val, at);

CREATE TABLE IF NOT EXISTS comments(
  id BIGSERIAL PRIMARY KEY,
  issue_id BIGINT REFERENCES issues(id) ON DELETE CASCADE,
  ext_id TEXT,
  author TEXT, at TIMESTAMPTZ, body TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_comments_issue_ext ON comments(issue_id, ext_id) WHERE ext_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS worklogs(
  id BIGSERIAL PRIMARY KEY,
  issue_id BIGINT REFERENCES issues(id) ON DELETE CASCADE,
  ext_id TEXT,
  author TEXT, started_at TIMESTAMPTZ, seconds INT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_worklogs_issue_ext ON worklogs(issue_id, ext_id) WHERE ext_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS findings(
  id BIGSERIAL PRIMARY KEY,
  issue_id BIGINT REFERENCES issues(id) ON DELETE CASCADE,
  week_start DATE, data JSONB
);

CREATE TABLE IF NOT EXISTS metrics_weekly(
  week_start DATE, squad TEXT, kpi TEXT, value NUMERIC,
  PRIMARY KEY (week_start, squad, kpi)
);
CREATE INDEX IF NOT EXISTS idx_metrics_weekly_week ON metrics_weekly(week_start);

-- Status map normalization per board
DO $$ BEGIN
    CREATE TYPE canonical_stage AS ENUM ('Backlog','Queue','InProgress','Review','Test','Deploy','Blocked','Done');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS status_map(
  board_id BIGINT,
  jira_status TEXT,
  canonical_stage canonical_stage,
  PRIMARY KEY(board_id, jira_status)
);

-- Job runs for observability
CREATE TABLE IF NOT EXISTS job_runs(
  id BIGSERIAL PRIMARY KEY,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  boards JSONB,
  issues_scanned INT,
  issues_llm INT,
  tokens_used INT,
  success BOOLEAN,
  error TEXT
);


