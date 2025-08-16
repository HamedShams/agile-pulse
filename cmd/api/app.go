package main

import (
    "bytes"
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "regexp"
    "sort"
    "strconv"
    "strings"
    "time"

    openai "github.com/openai/openai-go/v2"
    "github.com/openai/openai-go/v2/option"
    "github.com/openai/openai-go/v2/shared"
    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
)

// ---- Config ----
type Config struct {
    AppEnv   string
    TZ       string
    HTTPAddr string

    DBDSN string

    PublicBaseURL string

    JiraBaseURL    string
    JiraPAT        string
    JiraUsername   string
    JiraPassword   string
    JiraProjects   []string
    JiraDefaultJQL string
    JiraAPIVersion string
    JiraBoardNames []string
    JiraExpediteJQL string
    JiraFieldsFile  string
    JiraFieldMap    map[string]string

    OpenAIKey     string
    OpenAIModel   string
    OpenAITimeout time.Duration

    TelegramToken         string
    TelegramWebhookSecret string
    TelegramChatIDs       []int64
    TelegramChatUsernames []string

    DigestCron     string
    MaxConcurrency int
    HTTPTimeout    time.Duration

    WorkersJira    int
    WorkersLLM     int
    LLMBudgetTokens int
    LLMMaxIssues    int
}

func getenv(key, def string) string { if v := os.Getenv(key); v != "" { return v }; return def }
func atoi(key string, def int) int { v := os.Getenv(key); if v == "" { return def }; n, err := strconv.Atoi(v); if err != nil { return def }; return n }
func dur(key string, def time.Duration) time.Duration { v := os.Getenv(key); if v == "" { return def }; d, err := time.ParseDuration(v); if err != nil { return def }; return d }
func parseInt64s(csv string) []int64 { if csv=="" {return nil}; parts := strings.Split(csv, ","); out := make([]int64,0,len(parts)); for _,p := range parts{p=strings.TrimSpace(p); if p==""{continue}; n,err:=strconv.ParseInt(p,10,64); if err==nil{out=append(out,n)}}; return out }
func parseStrings(csv string) []string { if csv=="" {return nil}; parts := strings.Split(csv, ","); out := make([]string,0,len(parts)); for _,p := range parts{p=strings.TrimSpace(p); if p!=""{out=append(out,p)}}; return out }

func Load() Config {
    cfg := Config{
        AppEnv:   getenv("APP_ENV", "dev"),
        TZ:       getenv("APP_TZ", "Asia/Tehran"),
        HTTPAddr: getenv("HTTP_ADDR", ":8080"),

        DBDSN: getenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/agilepulse?sslmode=disable"),

        PublicBaseURL: getenv("PUBLIC_BASE_URL", "http://localhost:8080"),

        JiraBaseURL:    getenv("JIRA_BASE_URL", ""),
        JiraPAT:        getenv("JIRA_PAT", ""),
        JiraUsername:   getenv("JIRA_USERNAME", ""),
        JiraPassword:   getenv("JIRA_PASSWORD", ""),
        JiraProjects:   strings.Split(strings.TrimSpace(getenv("JIRA_PROJECTS", "SNAPPDR")), ","),
        JiraDefaultJQL: getenv("JIRA_DEFAULT_JQL", "updated >= -7d"),
        JiraAPIVersion: getenv("JIRA_API_VERSION", "2"),
        JiraBoardNames: parseStrings(getenv("JIRA_BOARD_NAMES", "")),
        JiraExpediteJQL: getenv("JIRA_EXPEDITE_JQL", ""),
        JiraFieldsFile:  getenv("JIRA_FIELDS_FILE", "/config/jira_fields.json"),

        OpenAIKey:     getenv("OPENAI_API_KEY", ""),
        OpenAIModel:   getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        OpenAITimeout: dur("OPENAI_TIMEOUT", 15*time.Second),

        TelegramToken:         getenv("TELEGRAM_BOT_TOKEN", ""),
        TelegramWebhookSecret: getenv("TELEGRAM_WEBHOOK_SECRET", "change-me"),
        TelegramChatIDs:       parseInt64s(getenv("TELEGRAM_CHAT_IDS", "")),
        TelegramChatUsernames: parseStrings(getenv("TELEGRAM_CHAT_USERNAMES", "")),

        DigestCron:     getenv("CRON_SPEC", "0 10 * * FRI"),
        MaxConcurrency: atoi("MAX_CONCURRENCY", 8),
        HTTPTimeout:    dur("HTTP_TIMEOUT", 15*time.Second),
        WorkersJira:    atoi("WORKERS_JIRA", 6),
        WorkersLLM:     atoi("WORKERS_LLM", 3),
        LLMBudgetTokens: atoi("LLM_TOKEN_BUDGET", 300000),
        LLMMaxIssues:    atoi("LLM_MAX_ISSUES", 60),
    }
    if len(cfg.TelegramChatIDs) == 0 {
        raw := strings.TrimSpace(getenv("TELEGRAM_CHAT_IDS", ""))
        if raw != "" {
            for _, r := range raw {
                if (r>='a'&&r<='z')||(r>='A'&&r<='Z')||r=='@'||r=='_' { cfg.TelegramChatUsernames = parseStrings(raw); break }
            }
        }
    }
    if loc, err := time.LoadLocation(cfg.TZ); err == nil { time.Local = loc }
    if data, err := os.ReadFile(cfg.JiraFieldsFile); err == nil {
        var arr []map[string]any
        if err := json.Unmarshal(data, &arr); err == nil {
            m := map[string]string{}
            for _, f := range arr { n,_ := f["name"].(string); id,_ := f["id"].(string); n=strings.TrimSpace(n); if n!=""&&id!=""{m[n]=id} }
            if len(m)>0 { cfg.JiraFieldMap = m }
        }
    } else if data2, err2 := os.ReadFile("config/jira_fields.json"); err2 == nil {
        var arr []map[string]any
        if err := json.Unmarshal(data2, &arr); err == nil {
            m := map[string]string{}
            for _, f := range arr { n,_ := f["name"].(string); id,_ := f["id"].(string); n=strings.TrimSpace(n); if n!=""&&id!=""{m[n]=id} }
            if len(m)>0 { cfg.JiraFieldMap = m }
        }
    }
    return cfg
}

// ---- Logger ----
func newLogger(cfg Config) zerolog.Logger {
    if cfg.AppEnv == "dev" {
        output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
        logger := zerolog.New(output).With().Timestamp().Logger()
        log.Logger = logger
        return logger
    }
    zerolog.TimeFieldFormat = time.RFC3339
    logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
    log.Logger = logger
    return logger
}

// ---- Domain ----
type Issue struct {
    ID             int64
    Key            string
    Project        string
    Type           string
    Priority       string
    Assignee       string
    Reporter       string
    StatusLast     string
    CreatedAtJira  *time.Time
    UpdatedAtJira  *time.Time
    DoneAt         *time.Time
    PointsEstimate *float64
    Squad          string
    DueAt          *time.Time
    Labels         []string
    Subteam        string
    Service        string
    EpicKey        string
}

type Event struct { ID int64; IssueID int64; Kind string; Field string; FromVal string; ToVal string; At time.Time }
type Comment struct { ID int64; IssueID int64; ExtID string; Author string; At time.Time; Body string }
type Worklog struct { ID int64; IssueID int64; ExtID string; Author string; StartedAt time.Time; Seconds int }
type Finding struct { ID int64; IssueID int64; WeekStart time.Time; Data map[string]any }

// ---- DB/Repository ----
type DB struct { Pool *pgxpool.Pool; log zerolog.Logger }
func MustOpen(ctx context.Context, cfg Config, logger zerolog.Logger) *DB {
    pool, err := pgxpool.New(ctx, cfg.DBDSN); if err != nil { logger.Fatal().Err(err).Msg("db connect failed") }
    ctx2, cancel := context.WithTimeout(ctx, 10*time.Second); defer cancel()
    if err := pool.Ping(ctx2); err != nil { logger.Fatal().Err(err).Msg("db ping failed") }
    return &DB{Pool: pool, log: logger}
}
func (d *DB) Close(){ d.Pool.Close() }

type Repository struct { db *DB; log zerolog.Logger }
func NewRepository(d *DB, logger zerolog.Logger) *Repository { return &Repository{db: d, log: logger} }

func (r *Repository) TryAdvisoryLock(ctx context.Context, key int64) (bool, error) { var ok bool; err := r.db.Pool.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&ok); return ok, err }
func (r *Repository) AdvisoryUnlock(ctx context.Context, key int64) error { var ok bool; err := r.db.Pool.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", key).Scan(&ok); if !ok && err==nil { return errors.New("advisory unlock returned false") }; return err }

func (r *Repository) UpsertIssue(ctx context.Context, i Issue) (int64, error) {
    const q = `
        INSERT INTO issues(key, project, type, priority, assignee, reporter, status_last,
            created_at_jira, updated_at_jira, done_at, points_estimate, squad,
            due_at, labels, subteam, service, epic_key)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
        ON CONFLICT(key) DO UPDATE SET
            project=EXCLUDED.project,
            type=EXCLUDED.type,
            priority=EXCLUDED.priority,
            assignee=EXCLUDED.assignee,
            reporter=EXCLUDED.reporter,
            status_last=EXCLUDED.status_last,
            created_at_jira=EXCLUDED.created_at_jira,
            updated_at_jira=EXCLUDED.updated_at_jira,
            done_at=EXCLUDED.done_at,
            points_estimate=EXCLUDED.points_estimate,
            squad=EXCLUDED.squad,
            due_at=EXCLUDED.due_at,
            labels=EXCLUDED.labels,
            subteam=EXCLUDED.subteam,
            service=EXCLUDED.service,
            epic_key=EXCLUDED.epic_key
        RETURNING id`
    var id int64
    row := r.db.Pool.QueryRow(ctx, q, i.Key, i.Project, i.Type, i.Priority, i.Assignee, i.Reporter, i.StatusLast,
        i.CreatedAtJira, i.UpdatedAtJira, i.DoneAt, i.PointsEstimate, i.Squad,
        i.DueAt, i.Labels, i.Subteam, i.Service, i.EpicKey)
    if err := row.Scan(&id); err != nil { return 0, err }
    return id, nil
}

func (r *Repository) BulkInsertEvents(ctx context.Context, ev []Event) error {
    if len(ev)==0 { return nil }
    batch := &pgx.Batch{}
    const q = `INSERT INTO events(issue_id, kind, field, from_val, to_val, at)
        VALUES($1,$2,$3,$4,$5,$6)
        ON CONFLICT (issue_id, field, from_val, to_val, at) DO NOTHING`
    for _, e := range ev { batch.Queue(q, e.IssueID, e.Kind, e.Field, e.FromVal, e.ToVal, e.At) }
    br := r.db.Pool.SendBatch(ctx, batch); defer br.Close()
    for range ev { if _, err := br.Exec(); err != nil { return err } }
    return nil
}

func (r *Repository) BulkInsertComments(ctx context.Context, c []Comment) error {
    if len(c)==0 { return nil }
    batch := &pgx.Batch{}
    const q = `INSERT INTO comments(issue_id, ext_id, author, at, body)
        VALUES($1,$2,$3,$4,$5)
        ON CONFLICT (issue_id, ext_id) DO NOTHING`
    for _, x := range c {
        var ext any; if strings.TrimSpace(x.ExtID)=="" { ext = nil } else { ext = x.ExtID }
        batch.Queue(q, x.IssueID, ext, x.Author, x.At, x.Body)
    }
    br := r.db.Pool.SendBatch(ctx, batch); defer br.Close()
    for range c { if _, err := br.Exec(); err != nil { return err } }
    return nil
}

func (r *Repository) BulkInsertWorklogs(ctx context.Context, wl []Worklog) error {
    if len(wl)==0 { return nil }
    batch := &pgx.Batch{}
    const q = `INSERT INTO worklogs(issue_id, ext_id, author, started_at, seconds)
        VALUES($1,$2,$3,$4,$5)
        ON CONFLICT (issue_id, ext_id) DO NOTHING`
    for _, x := range wl {
        var ext any; if strings.TrimSpace(x.ExtID)=="" { ext = nil } else { ext = x.ExtID }
        batch.Queue(q, x.IssueID, ext, x.Author, x.StartedAt, x.Seconds)
    }
    br := r.db.Pool.SendBatch(ctx, batch); defer br.Close()
    for range wl { if _, err := br.Exec(); err != nil { return err } }
    return nil
}

// KPIs
func (r *Repository) ComputeWeeklyMetrics(ctx context.Context, weekStart time.Time, squad string) (map[string]float64, error) {
    weekEnd := weekStart.Add(7*24*time.Hour)
    metrics := map[string]float64{}
    type rowDone struct{ ID int64; Project string; Created, Done *time.Time; Type string }
    var doneIssues []rowDone
    rows, err := r.db.Pool.Query(ctx, `SELECT id, COALESCE(project,''), created_at_jira, done_at, COALESCE(type,'') FROM issues WHERE done_at >= $1 AND done_at < $2`, weekStart, weekEnd)
    if err != nil { return nil, err }
    defer rows.Close()
    for rows.Next(){ var id int64; var proj, typ string; var cr, dn *time.Time; if err := rows.Scan(&id,&proj,&cr,&dn,&typ); err!=nil { return nil, err }; doneIssues = append(doneIssues, rowDone{ID:id, Project:proj, Created:cr, Done:dn, Type:strings.ToLower(typ)}) }
    metrics["throughput_total"] = float64(len(doneIssues))
    if len(doneIssues)>0 { countBy := map[string]int{}; for _,d := range doneIssues { countBy[d.Type]++ }; for typ,c := range countBy { key := fmt.Sprintf("throughput_%s", strings.ReplaceAll(typ, " ", "_")); metrics[key] = float64(c) } }
    var wipCount float64
    if err := r.db.Pool.QueryRow(ctx, `SELECT COALESCE(COUNT(*),0) FROM issues WHERE done_at IS NULL`).Scan(&wipCount); err != nil { return nil, err }
    metrics["wip_count"] = wipCount
    var leadAvgHours *float64
    if err := r.db.Pool.QueryRow(ctx, `SELECT AVG(EXTRACT(EPOCH FROM (done_at - created_at_jira))/3600.0)
        FROM issues WHERE done_at IS NOT NULL AND created_at_jira IS NOT NULL AND done_at >= $1 AND done_at < $2`, weekStart, weekEnd).Scan(&leadAvgHours); err==nil && leadAvgHours!=nil {
        metrics["lead_time_days_avg"] = *leadAvgHours / 24.0
    } else { metrics["lead_time_days_avg"] = 0 }
    canonicalMap := map[string]string{}
    if rows2, err := r.db.Pool.Query(ctx, `SELECT DISTINCT jira_status, canonical_stage::text FROM status_map`); err==nil {
        defer rows2.Close(); for rows2.Next(){ var js,st string; if err := rows2.Scan(&js,&st); err==nil { canonicalMap[strings.ToLower(strings.TrimSpace(js))]=st } }
    }
    toCanonical := func(status string) string {
        key := strings.ToLower(strings.TrimSpace(status)); if v, ok := canonicalMap[key]; ok && v!="" { return v }
        switch {
        case key=="backlog": return "Backlog"
        case strings.Contains(key, "to do") || key=="todo": return "Queue"
        case strings.Contains(key, "in progress") || key=="doing": return "InProgress"
        case strings.Contains(key, "review") || strings.Contains(key, "ready4test"): return "Review"
        case strings.Contains(key, "test") || strings.Contains(key, "qa"): return "Test"
        case strings.Contains(key, "deploy") || strings.Contains(key, "release"): return "Deploy"
        case strings.Contains(key, "block") || key=="pending": return "Blocked"
        case strings.Contains(key, "done") || strings.Contains(key, "resolve"): return "Done"
        default: return "Queue"
        }
    }
    type statusEvent struct{ IssueID int64; Field string; From string; To string; At time.Time }
    loadEvents := func(ids []int64) ([]statusEvent, error) {
        if len(ids)==0 { return nil, nil }
        rows, err := r.db.Pool.Query(ctx, `SELECT issue_id, field, from_val, to_val, at FROM events WHERE field IN ('status','Flagged') ORDER BY issue_id, at`)
        if err != nil { return nil, err }
        defer rows.Close()
        set := map[int64]struct{}{}; for _, id := range ids { set[id]=struct{}{} }
        var out []statusEvent
        for rows.Next(){ var eid int64; var field string; var from, to *string; var at time.Time; if err := rows.Scan(&eid,&field,&from,&to,&at); err!=nil { return nil, err }
            if _,ok := set[eid]; !ok { continue }
            fv := ""; if from!=nil { fv = *from }; tv := ""; if to!=nil { tv = *to }
            out = append(out, statusEvent{IssueID:eid, Field:field, From:fv, To:tv, At:at}) }
        return out, nil
    }
    if len(doneIssues)>0 {
        ids := make([]int64,0,len(doneIssues)); for _,d := range doneIssues { ids = append(ids, d.ID) }
        evts, err := loadEvents(ids); if err != nil { return nil, err }
        byIssue := map[int64][]statusEvent{}; for _,e := range evts { byIssue[e.IssueID] = append(byIssue[e.IssueID], e) }
        var cycleHours []float64; stageHours := map[string]float64{}; flipsCount := 0; blockersHours := 0.0; flaggedHours := 0.0
        for _, d := range doneIssues {
            seq := byIssue[d.ID]; sort.Slice(seq, func(i,j int) bool { return seq[i].At.Before(seq[j].At) })
            type seg struct{ Stage string; Start, End time.Time }
            var segments []seg; var curStage string; var curStart time.Time
            var statusSeq []statusEvent; for _,e := range seq { if strings.EqualFold(e.Field, "status") { statusSeq = append(statusSeq, e) } }
            if len(statusSeq)>0 { curStage = toCanonical(statusSeq[0].From); if d.Created!=nil { curStart = *d.Created } else { curStart = seq[0].At }
                for _, e := range statusSeq { nextStage := toCanonical(e.To); end := e.At; if !end.Before(curStart) { segments = append(segments, seg{Stage:curStage, Start:curStart, End:end}) }; curStage = nextStage; curStart = e.At } }
            if d.Done!=nil && !d.Done.Before(curStart) { segments = append(segments, seg{Stage:curStage, Start:curStart, End:*d.Done}) }
            firstInProgress := time.Time{}
            for _, s := range segments { durH := s.End.Sub(s.Start).Hours(); if durH<0 { continue }; stageHours[s.Stage]+=durH; if s.Stage=="InProgress" && firstInProgress.IsZero() { firstInProgress = s.Start }; if s.Stage=="Blocked" { blockersHours += durH } }
            on := false; var onStart time.Time
            for _,e := range seq { if !strings.EqualFold(e.Field, "Flagged") { continue }; nonEmpty := strings.TrimSpace(e.To) != "" && !strings.EqualFold(strings.TrimSpace(e.To), "none"); if nonEmpty && !on { on = true; onStart = e.At } else if !nonEmpty && on { on = false; if d.Done!=nil && e.At.After(*d.Done) { flaggedHours += d.Done.Sub(onStart).Hours() } else { flaggedHours += e.At.Sub(onStart).Hours() } } }
            if on && d.Done!=nil { flaggedHours += d.Done.Sub(onStart).Hours() }
            if !firstInProgress.IsZero() && d.Done!=nil { cycleHours = append(cycleHours, d.Done.Sub(firstInProgress).Hours()) }
            flips := 0; prev := ""; for _,e := range statusSeq { to := toCanonical(e.To); if (prev=="InProgress" && (to=="Review"||to=="Test")) || ((prev=="Review"||prev=="Test") && to=="InProgress") { flips++ }; prev = to }; if flips>=2 { flipsCount++ }
        }
        if len(cycleHours)>0 { sum := 0.0; for _,h := range cycleHours { sum+=h }; metrics["cycle_time_days_avg"] = (sum/float64(len(cycleHours)))/24.0 } else { metrics["cycle_time_days_avg"] = 0 }
        totalIssues := float64(len(doneIssues)); for _, stage := range []string{"Backlog","Queue","InProgress","Review","Test","Deploy","Blocked"} { key := fmt.Sprintf("queue_time_days_%s", stage); metrics[key] = (stageHours[stage]/totalIssues)/24.0 }
        metrics["ping_pong_ge2_issues"] = float64(flipsCount)
        metrics["blocker_dwell_days_avg"] = (blockersHours/float64(maxi(1, len(doneIssues))))/24.0
        if flaggedHours>0 { metrics["flagged_dwell_days_avg"] = (flaggedHours/float64(maxi(1, len(doneIssues))))/24.0 } else { metrics["flagged_dwell_days_avg"] = 0 }
    } else {
        metrics["cycle_time_days_avg"] = 0; for _, stage := range []string{"Backlog","Queue","InProgress","Review","Test","Deploy","Blocked"} { metrics["queue_time_days_"+stage] = 0 } ; metrics["ping_pong_ge2_issues"] = 0; metrics["blocker_dwell_days_avg"] = 0
    }
    // WIP age
    rows3, err := r.db.Pool.Query(ctx, `SELECT id FROM issues WHERE done_at IS NULL`); if err != nil { return nil, err }
    defer rows3.Close(); var ids []int64; for rows3.Next(){ var id int64; if err := rows3.Scan(&id); err!=nil { return nil, err }; ids = append(ids, id) }
    if len(ids)>0 {
        rows4, err := r.db.Pool.Query(ctx, `SELECT issue_id, from_val, to_val, at FROM events WHERE field = 'status' ORDER BY issue_id, at`); if err != nil { return nil, err }
        defer rows4.Close(); var evts []struct{ IssueID int64; From, To *string; At time.Time }
        for rows4.Next(){ var eid int64; var from, to *string; var at time.Time; if err := rows4.Scan(&eid,&from,&to,&at); err != nil { return nil, err }; evts = append(evts, struct{IssueID int64; From, To *string; At time.Time}{eid, from, to, at}) }
        byIssue := map[int64][]struct{ IssueID int64; From, To *string; At time.Time }{}; for _,e := range evts { byIssue[e.IssueID] = append(byIssue[e.IssueID], e) }
        now := time.Now().UTC(); count := 0; sumHours := 0.0
        for _, id := range ids { seq := byIssue[id]; sort.Slice(seq, func(i,j int) bool { return seq[i].At.Before(seq[j].At) }); entered := time.Time{}; cur := ""; for _,e := range seq { to := strings.ToLower(strings.TrimSpace(derefString(e.To))); canon := to; if strings.Contains(to, "in progress") || to=="doing" { canon = "InProgress" }; if cur!="InProgress" && canon=="InProgress" { entered = e.At; break }; cur = canon }; if !entered.IsZero() { sumHours += now.Sub(entered).Hours(); count++ } }
        if count>0 { metrics["wip_age_days_avg"] = (sumHours/float64(count))/24.0 } else { metrics["wip_age_days_avg"] = 0 }
    } else { metrics["wip_age_days_avg"] = 0 }
    return metrics, nil
}

func maxi(a,b int) int { if a>b { return a }; return b }

func (r *Repository) BulkInsertMetrics(ctx context.Context, weekStart time.Time, squad string, metrics map[string]float64) error {
    if len(metrics)==0 { return nil }
    batch := &pgx.Batch{}
    const q = `INSERT INTO metrics_weekly(week_start, squad, kpi, value) VALUES($1,$2,$3,$4)
        ON CONFLICT (week_start, squad, kpi) DO UPDATE SET value=EXCLUDED.value`
    for k,v := range metrics { batch.Queue(q, weekStart, squad, k, v) }
    br := r.db.Pool.SendBatch(ctx, batch); defer br.Close(); for range metrics { if _, err := br.Exec(); err != nil { return err } }; return nil
}

func (r *Repository) GetWeeklyMetrics(ctx context.Context, weekStart time.Time, squad string) (map[string]float64, error) {
    rows, err := r.db.Pool.Query(ctx, `SELECT kpi, value FROM metrics_weekly WHERE week_start=$1 AND squad=$2`, weekStart, squad); if err != nil { return nil, err }
    defer rows.Close(); out := map[string]float64{}; for rows.Next(){ var k string; var v float64; if err := rows.Scan(&k,&v); err!=nil { return nil, err }; out[k]=v }; return out, nil
}

func (r *Repository) ComputeWeeklyProjectCounts(ctx context.Context, weekStart time.Time) (map[string]map[string]float64, error) {
    weekEnd := weekStart.Add(7*24*time.Hour); out := map[string]map[string]float64{}
    rows, err := r.db.Pool.Query(ctx, `SELECT COALESCE(project,''), COUNT(*) FROM issues WHERE done_at >= $1 AND done_at < $2 GROUP BY 1`, weekStart, weekEnd); if err != nil { return nil, err }
    defer rows.Close(); for rows.Next(){ var proj string; var c float64; if err := rows.Scan(&proj,&c); err!=nil { return nil, err }; if _,ok := out[proj]; !ok { out[proj]=map[string]float64{} }; out[proj]["throughput_total"] = c }
    rows2, err := r.db.Pool.Query(ctx, `SELECT COALESCE(project,''), COUNT(*) FROM issues WHERE done_at IS NULL GROUP BY 1`); if err != nil { return nil, err }
    defer rows2.Close(); for rows2.Next(){ var proj string; var c float64; if err := rows2.Scan(&proj,&c); err!=nil { return nil, err }; if _,ok := out[proj]; !ok { out[proj]=map[string]float64{} }; out[proj]["wip_count"] = c }
    return out, nil
}

type LastRun struct { StartedAt time.Time `json:"started_at"`; FinishedAt *time.Time `json:"finished_at"`; Boards string `json:"boards"`; IssuesScanned int `json:"issues_scanned"`; IssuesLLM int `json:"issues_llm"`; TokensUsed int `json:"tokens_used"`; Success bool `json:"success"`; Error string `json:"error"` }
func (r *Repository) StartJobRun(ctx context.Context, boardsJSON string) (int64, error) { const q = `INSERT INTO job_runs(started_at, boards, success) VALUES(now(), $1, false) RETURNING id`; var id int64; if err := r.db.Pool.QueryRow(ctx, q, boardsJSON).Scan(&id); err != nil { return 0, err }; return id, nil }
func (r *Repository) FinishJobRun(ctx context.Context, id int64, issuesScanned, issuesLLM, tokensUsed int, success bool, errStr string) error { const q = `UPDATE job_runs SET finished_at=now(), issues_scanned=$2, issues_llm=$3, tokens_used=$4, success=$5, error=$6 WHERE id=$1`; _, err := r.db.Pool.Exec(ctx, q, id, issuesScanned, issuesLLM, tokensUsed, success, errStr); return err }
func (r *Repository) GetLastRun(ctx context.Context) (*LastRun, error) { const q = `SELECT started_at, finished_at, boards::text, coalesce(issues_scanned,0), coalesce(issues_llm,0), coalesce(tokens_used,0), coalesce(success,false), coalesce(error,'') FROM job_runs ORDER BY id DESC LIMIT 1`; row := r.db.Pool.QueryRow(ctx, q); lr := &LastRun{}; if err := row.Scan(&lr.StartedAt, &lr.FinishedAt, &lr.Boards, &lr.IssuesScanned, &lr.IssuesLLM, &lr.TokensUsed, &lr.Success, &lr.Error); err != nil { return nil, err }; return lr, nil }

// ---- Jira Client ----
type JiraClient struct { baseURL, token, basic, user, pass, apiVer string; http *http.Client; log zerolog.Logger }
func NewJiraClient(cfg Config, logger zerolog.Logger) *JiraClient { return &JiraClient{ baseURL: cfg.JiraBaseURL, token: cfg.JiraPAT, basic: getenvBasic(), user: cfg.JiraUsername, pass: cfg.JiraPassword, http: &http.Client{ Timeout: cfg.HTTPTimeout }, log: logger, apiVer: cfg.JiraAPIVersion } }
func getenvBasic() string { v := ""; if s := strings.TrimSpace(os.Getenv("JIRA_BASIC_AUTH")); s!="" { v=s }; return v }
func (c *JiraClient) apiURL(path string, q url.Values) string { base := strings.TrimRight(c.baseURL, "/"); if !strings.HasPrefix(path, "/") { path = "/"+path }; u := base+path; if q!=nil && len(q)>0 { u = u+"?"+q.Encode() }; return u }
func (c *JiraClient) doJSON(ctx context.Context, method, u string, body any) (map[string]any, error) { if c.baseURL=="" { return nil, errors.New("jira: empty baseURL") }; var r io.Reader; if body!=nil { b,err := json.Marshal(body); if err!=nil { return nil, err }; r = strings.NewReader(string(b)) }; var lastErr error; for attempt:=0; attempt<3; attempt++ { req, err := http.NewRequestWithContext(ctx, method, u, r); if err!=nil { return nil, err }; if body!=nil { req.Header.Set("Content-Type","application/json") }; if c.token!="" { req.Header.Set("Authorization","Bearer "+c.token) } else if c.user!="" && c.pass!="" { req.SetBasicAuth(c.user, c.pass) } else if c.basic!="" { req.Header.Set("Authorization","Basic "+c.basic) } ; resp, err := c.http.Do(req); if err!=nil { lastErr=err } else { defer resp.Body.Close(); if resp.StatusCode>=300 { b,_ := io.ReadAll(resp.Body); if resp.StatusCode==429 || resp.StatusCode>=500 { lastErr = fmt.Errorf("jira api status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b))) } else { return nil, fmt.Errorf("jira api status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b))) } } else { var out map[string]any; if err := json.NewDecoder(resp.Body).Decode(&out); err!=nil { return nil, err }; return out, nil } }; time.Sleep(time.Duration(300*(1<<attempt))*time.Millisecond) }; return nil, lastErr }
func (c *JiraClient) Issue(ctx context.Context, key string, expandChangelog bool) (any, error) { if key=="" { return nil, errors.New("jira: empty issue key") }; q := url.Values{}; q.Set("fields","*all"); if expandChangelog { q.Set("expand","changelog") }; path := "/rest/api/3/issue/"+url.PathEscape(key); if c.apiVer=="2" { path = "/rest/api/2/issue/"+url.PathEscape(key) }; u := c.apiURL(path, q); return c.doJSON(ctx, http.MethodGet, u, nil) }
func (c *JiraClient) Search(ctx context.Context, jql string, startAt, max int) (any, error) { if jql=="" { return nil, errors.New("jira: empty jql") }; if c.apiVer=="2" { q := url.Values{}; q.Set("jql", jql); if startAt>0 { q.Set("startAt", fmt.Sprint(startAt)) }; if max>0 { q.Set("maxResults", fmt.Sprint(max)) }; q.Set("fields","*all"); u := c.apiURL("/rest/api/2/search", q); return c.doJSON(ctx, http.MethodGet, u, nil) }; body := map[string]any{"jql":jql,"startAt":startAt,"maxResults":max}; u := c.apiURL("/rest/api/3/search", url.Values{"fields": []string{"*all"}}); return c.doJSON(ctx, http.MethodPost, u, body) }
func (c *JiraClient) Comments(ctx context.Context, key string, startAt, max int) (any, error) { if key=="" { return nil, errors.New("jira: empty issue key") }; q := url.Values{}; if startAt>0 { q.Set("startAt", fmt.Sprint(startAt)) }; if max>0 { q.Set("maxResults", fmt.Sprint(max)) }; path := "/rest/api/3/issue/"+url.PathEscape(key)+"/comment"; if c.apiVer=="2" { path = "/rest/api/2/issue/"+url.PathEscape(key)+"/comment" }; u := c.apiURL(path, q); return c.doJSON(ctx, http.MethodGet, u, nil) }
func (c *JiraClient) Changelog(ctx context.Context, key string, startAt, max int) (any, error) { if key=="" { return nil, errors.New("jira: empty issue key") }; q := url.Values{}; if startAt>0 { q.Set("startAt", fmt.Sprint(startAt)) }; if max>0 { q.Set("maxResults", fmt.Sprint(max)) }; path := "/rest/api/3/issue/"+url.PathEscape(key)+"/changelog"; if c.apiVer=="2" { path = "/rest/api/2/issue/"+url.PathEscape(key)+"/changelog" }; u := c.apiURL(path, q); return c.doJSON(ctx, http.MethodGet, u, nil) }
func (c *JiraClient) Worklogs(ctx context.Context, key string, startAt, max int) (any, error) { if key=="" { return nil, errors.New("jira: empty issue key") }; q := url.Values{}; if startAt>0 { q.Set("startAt", fmt.Sprint(startAt)) }; if max>0 { q.Set("maxResults", fmt.Sprint(max)) }; path := "/rest/api/3/issue/"+url.PathEscape(key)+"/worklog"; if c.apiVer=="2" { path = "/rest/api/2/issue/"+url.PathEscape(key)+"/worklog" }; u := c.apiURL(path, q); return c.doJSON(ctx, http.MethodGet, u, nil) }
func (c *JiraClient) Boards(ctx context.Context, startAt, max int) (any, error) { q := url.Values{}; if startAt>0 { q.Set("startAt", fmt.Sprint(startAt)) }; if max>0 { q.Set("maxResults", fmt.Sprint(max)) }; u := c.apiURL("/rest/agile/1.0/board", q); return c.doJSON(ctx, http.MethodGet, u, nil) }
func (c *JiraClient) BoardIssues(ctx context.Context, boardID int64, startAt, max int, jql string) (any, error) { if boardID<=0 { return nil, errors.New("jira: invalid board id") }; q := url.Values{}; if startAt>0 { q.Set("startAt", fmt.Sprint(startAt)) }; if max>0 { q.Set("maxResults", fmt.Sprint(max)) }; if strings.TrimSpace(jql)!="" { q.Set("jql", jql) }; q.Set("fields","*all"); path := "/rest/agile/1.0/board/"+strconv.FormatInt(boardID,10)+"/issue"; u := c.apiURL(path, q); return c.doJSON(ctx, http.MethodGet, u, nil) }
func (c *JiraClient) Fields(ctx context.Context) (any, error) { u := c.apiURL("/rest/api/2/field", nil); req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil); if err!=nil { return nil, err }; if c.token!="" { req.Header.Set("Authorization","Bearer "+c.token) }; if c.user!="" && c.pass!="" { req.SetBasicAuth(c.user, c.pass) }; if c.basic!="" { req.Header.Set("Authorization","Basic "+c.basic) }; resp, err := c.http.Do(req); if err!=nil { return nil, err }; defer resp.Body.Close(); if resp.StatusCode>=300 { b,_ := io.ReadAll(resp.Body); return nil, fmt.Errorf("jira api status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b))) }; var out []map[string]any; if err := json.NewDecoder(resp.Body).Decode(&out); err!=nil { return nil, err }; return out, nil }

// ---- Telegram Client ----
type TelegramClient struct { token string; http *http.Client; log zerolog.Logger }
func NewTelegramClient(cfg Config, logger zerolog.Logger) *TelegramClient { return &TelegramClient{ token: cfg.TelegramToken, http: &http.Client{ Timeout: 10*time.Second }, log: logger } }
func (c *TelegramClient) SendMessage(ctx context.Context, chatID int64, text string) error { if c.token==""||chatID==0 { return fmt.Errorf("telegram: missing token or chat id") }; url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", c.token); body := map[string]any{"chat_id":chatID, "text":text, "parse_mode":"Markdown", "disable_web_page_preview":true}; b,_ := json.Marshal(body); req,_ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b)); req.Header.Set("Content-Type","application/json"); resp, err := c.http.Do(req); if err!=nil { return err }; defer resp.Body.Close(); if resp.StatusCode>=300 { bb,_ := io.ReadAll(resp.Body); return fmt.Errorf("telegram sendMessage status=%d body=%s", resp.StatusCode, string(bb)) }; return nil }
func (c *TelegramClient) SendMessagePlain(ctx context.Context, chatID int64, text string) error { if c.token==""||chatID==0 { return fmt.Errorf("telegram: missing token or chat id") }; url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", c.token); body := map[string]any{"chat_id":chatID, "text":text, "disable_web_page_preview":true}; b,_ := json.Marshal(body); req,_ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b)); req.Header.Set("Content-Type","application/json"); resp, err := c.http.Do(req); if err!=nil { return err }; defer resp.Body.Close(); if resp.StatusCode>=300 { bb,_ := io.ReadAll(resp.Body); return fmt.Errorf("telegram sendMessage status=%d body=%s", resp.StatusCode, string(bb)) }; return nil }
func (c *TelegramClient) SendMarkdownV2(ctx context.Context, chatID int64, text string) error { if c.token==""||chatID==0 { return fmt.Errorf("telegram: missing token or chat id") }; url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", c.token); body := map[string]any{"chat_id":chatID, "text":text, "parse_mode":"MarkdownV2", "disable_web_page_preview":true}; b,_ := json.Marshal(body); req,_ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b)); req.Header.Set("Content-Type","application/json"); resp, err := c.http.Do(req); if err!=nil { return err }; defer resp.Body.Close(); if resp.StatusCode>=300 { bb,_ := io.ReadAll(resp.Body); return fmt.Errorf("telegram sendMessage status=%d body=%s", resp.StatusCode, string(bb)) }; return nil }
func (c *TelegramClient) ResolveUsername(ctx context.Context, username string) (int64, error) { if c.token==""||username=="" { return 0, fmt.Errorf("telegram: missing token or username") }; url := fmt.Sprintf("https://api.telegram.org/bot%s/getChat", c.token); body := map[string]any{"chat_id": username}; b,_ := json.Marshal(body); req,_ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b)); req.Header.Set("Content-Type","application/json"); resp, err := c.http.Do(req); if err!=nil { return 0, err }; defer resp.Body.Close(); if resp.StatusCode>=300 { return 0, fmt.Errorf("telegram getChat status=%d", resp.StatusCode) }; var r struct{ OK bool `json:"ok"`; Result struct{ ID int64 `json:"id"` } `json:"result"` }; if err := json.NewDecoder(resp.Body).Decode(&r); err!=nil { return 0, err }; if !r.OK || r.Result.ID==0 { return 0, fmt.Errorf("telegram: invalid getChat response") }; return r.Result.ID, nil }
func (c *TelegramClient) SetWebhook(ctx context.Context, webhookURL string, secretToken string) error { if c.token==""||webhookURL==""||secretToken=="" { return fmt.Errorf("telegram: missing token, url or secret") }; url := fmt.Sprintf("https://api.telegram.org/bot%s/setWebhook", c.token); body := map[string]any{"url": webhookURL, "secret_token": secretToken, "drop_pending_updates": true, "allowed_updates": []string{"message","callback_query"}}; b,_ := json.Marshal(body); req,_ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b)); req.Header.Set("Content-Type","application/json"); resp, err := c.http.Do(req); if err!=nil { return err }; defer resp.Body.Close(); if resp.StatusCode>=300 { return fmt.Errorf("telegram setWebhook status=%d", resp.StatusCode) }; return nil }

// ---- OpenAI Client ----
type OpenAIClient struct { key, model string; cli openai.Client; log zerolog.Logger }
func NewOpenAIClient(cfg Config, logger zerolog.Logger) *OpenAIClient { model := cfg.OpenAIModel; if strings.TrimSpace(model)=="" { model = "gpt-4.1-mini" }; cli := openai.NewClient(option.WithAPIKey(cfg.OpenAIKey)); return &OpenAIClient{ key: cfg.OpenAIKey, model: model, cli: cli, log: logger } }
func (c *OpenAIClient) ExtractIssue(ctx context.Context, payload any) (map[string]any, error) { if strings.TrimSpace(c.key)=="" { return nil, errors.New("openai: missing key") }; c.log.Info().Str("model", c.model).Msg("openai ExtractIssue call"); userContent := ""; if b,err := json.Marshal(payload); err==nil { userContent = string(b) }; params := openai.ChatCompletionNewParams{ Model: shared.ChatModel(c.model), Messages: []openai.ChatCompletionMessageParamUnion{ openai.SystemMessage("You are an agile analyst. Extract blockers, QA churn, root causes, risks, and notes from this issue payload. Return concise JSON with keys: blockers[], qa_churn, suspected_causes[], risks[], notes[]."), openai.UserMessage(userContent), }, }; resp, err := c.cli.Chat.Completions.New(ctx, params); if err!=nil { return nil, err }; if len(resp.Choices)==0 { return nil, errors.New("openai: no choices") }; var m map[string]any; if err := json.Unmarshal([]byte(resp.Choices[0].Message.Content), &m); err!=nil { return nil, err }; return m, nil }
func (c *OpenAIClient) Summarize(ctx context.Context, kpis map[string]float64, findings []map[string]any) (string, error) { if strings.TrimSpace(c.key)=="" { return "", errors.New("openai: missing key") }; c.log.Info().Str("model", c.model).Msg("openai Summarize call"); payload := map[string]any{"kpis":kpis, "findings":findings}; userContent := ""; if b,err := json.Marshal(payload); err==nil { userContent = string(b) }; params := openai.ChatCompletionNewParams{ Model: shared.ChatModel(c.model), Messages: []openai.ChatCompletionMessageParamUnion{ openai.SystemMessage("You are a senior agile coach. Given KPIs and extracted findings, produce a concise, actionable weekly summary with anomalies and suggested actions."), openai.UserMessage(userContent), }, }; resp, err := c.cli.Chat.Completions.New(ctx, params); if err!=nil { return "", err }; if len(resp.Choices)==0 { return "", errors.New("openai: no choices") }; return resp.Choices[0].Message.Content, nil }

// ---- Utilities ----
func parseTimeUTC(v any) *time.Time { s, _ := v.(string); if s=="" { return nil }; layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000-0700", "2006-01-02T15:04:05-0700"}; for _,l := range layouts { if t,err := time.Parse(l, s); err==nil { tt := t.UTC(); return &tt } }; return nil }
func toStrAny(v any) string { if v==nil { return "" }; if s,ok := v.(string); ok { return s }; return fmt.Sprintf("%v", v) }
func optionToString(v any) string { if v==nil { return "" }; switch t := v.(type) { case string: return t; case map[string]any: if s,ok := t["value"].(string); ok { return s }; if name,ok := t["name"].(string); ok { return name }; return toStrAny(v); case []any: vals := make([]string,0,len(t)); for _,it := range t { switch m := it.(type) { case map[string]any: if s,ok := m["value"].(string); ok { vals = append(vals, s); continue }; if name,ok := m["name"].(string); ok { vals = append(vals, name); continue }; case string: vals = append(vals, m) } }; return strings.Join(vals, ", "); default: return toStrAny(v) } }
func sanitizeJiraText(s string) string { if s=="" { return s }; replacers := []struct{ old, new string }{{"\r\n","\n"},{"\r","\n"},{"{code}",""},{"{noformat}",""},{"{panel}",""},{"{color:#ff0000}",""},{"{color}",""}}; out := s; for _,r := range replacers { out = strings.ReplaceAll(out, r.old, r.new) }; if idx := strings.Index(out, "{code:"); idx != -1 { out = strings.ReplaceAll(out, "{code:java}", ""); out = strings.ReplaceAll(out, "{code:json}", ""); out = strings.ReplaceAll(out, "{code:sql}", "") }; return out }
func derefTime(t *time.Time) time.Time { if t==nil { return time.Time{} }; return *t }
func derefString(s *string) string { if s==nil { return "" }; return *s }


