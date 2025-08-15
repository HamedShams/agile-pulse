package repo

import (
    "context"
    "errors"
	"fmt"
    "sort"
    "strings"
    "time"

    "github.com/HamedShams/agile-pulse/internal/config"
    "github.com/HamedShams/agile-pulse/internal/domain"
    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/rs/zerolog"
)

type DB struct {
    Pool *pgxpool.Pool
    log  zerolog.Logger
}

func MustOpen(ctx context.Context, cfg config.Config, log zerolog.Logger) *DB {
    pool, err := pgxpool.New(ctx, cfg.DBDSN)
    if err != nil { log.Fatal().Err(err).Msg("db connect failed") }
    ctx2, cancel := context.WithTimeout(ctx, 10*time.Second); defer cancel()
    if err := pool.Ping(ctx2); err != nil { log.Fatal().Err(err).Msg("db ping failed") }
    return &DB{Pool: pool, log: log}
}

func (d *DB) Close() { d.Pool.Close() }

type Repository struct {
    db  *DB
    log zerolog.Logger
}

func NewRepository(d *DB, log zerolog.Logger) *Repository { return &Repository{db: d, log: log} }

func (r *Repository) TryAdvisoryLock(ctx context.Context, key int64) (bool, error) {
    var ok bool
    err := r.db.Pool.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&ok)
    return ok, err
}

func (r *Repository) AdvisoryUnlock(ctx context.Context, key int64) error {
    var ok bool
    err := r.db.Pool.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", key).Scan(&ok)
    if !ok && err == nil { return errors.New("advisory unlock returned false") }
    return err
}

// Stubs to be implemented
func (r *Repository) UpsertIssue(ctx context.Context, i domain.Issue) (int64, error) {
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

func (r *Repository) BulkInsertEvents(ctx context.Context, ev []domain.Event) error {
	if len(ev) == 0 { return nil }
    batch := &pgx.Batch{}
    const q = `INSERT INTO events(issue_id, kind, field, from_val, to_val, at)
        VALUES($1,$2,$3,$4,$5,$6)
        ON CONFLICT (issue_id, field, from_val, to_val, at) DO NOTHING`
	for _, e := range ev {
		batch.Queue(q, e.IssueID, e.Kind, e.Field, e.FromVal, e.ToVal, e.At)
	}
    br := r.db.Pool.SendBatch(ctx, batch)
	defer br.Close()
    for range ev { if _, err := br.Exec(); err != nil { return err } }
	return nil
}

func (r *Repository) BulkInsertComments(ctx context.Context, c []domain.Comment) error {
	if len(c) == 0 { return nil }
    batch := &pgx.Batch{}
    const q = `INSERT INTO comments(issue_id, ext_id, author, at, body)
        VALUES($1,$2,$3,$4,$5)
        ON CONFLICT (issue_id, ext_id) DO NOTHING`
    for _, x := range c { 
        var ext any
        if strings.TrimSpace(x.ExtID) == "" { ext = nil } else { ext = x.ExtID }
        batch.Queue(q, x.IssueID, ext, x.Author, x.At, x.Body) 
    }
    br := r.db.Pool.SendBatch(ctx, batch)
	defer br.Close()
    for range c { if _, err := br.Exec(); err != nil { return err } }
	return nil
}

func (r *Repository) BulkInsertWorklogs(ctx context.Context, wl []domain.Worklog) error {
	if len(wl) == 0 { return nil }
    batch := &pgx.Batch{}
    const q = `INSERT INTO worklogs(issue_id, ext_id, author, started_at, seconds)
        VALUES($1,$2,$3,$4,$5)
        ON CONFLICT (issue_id, ext_id) DO NOTHING`
    for _, x := range wl { 
        var ext any
        if strings.TrimSpace(x.ExtID) == "" { ext = nil } else { ext = x.ExtID }
        batch.Queue(q, x.IssueID, ext, x.Author, x.StartedAt, x.Seconds) 
    }
    br := r.db.Pool.SendBatch(ctx, batch)
	defer br.Close()
	for range wl { if _, err := br.Exec(); err != nil { return err } }
	return nil
}
func (r *Repository) ComputeWeeklyMetrics(ctx context.Context, weekStart time.Time, squad string) (map[string]float64, error) {
    // Window
    weekEnd := weekStart.Add(7 * 24 * time.Hour)
    metrics := map[string]float64{}

    // Throughput: total and by type for issues done this week (capture project for per-project aggregation)
    type rowDone struct{ ID int64; Project string; Created, Done *time.Time; Type string }
    var doneIssues []rowDone
    {
        rows, err := r.db.Pool.Query(ctx, `SELECT id, COALESCE(project,''), created_at_jira, done_at, COALESCE(type,'') FROM issues WHERE done_at >= $1 AND done_at < $2`, weekStart, weekEnd)
        if err != nil { return nil, err }
        defer rows.Close()
        for rows.Next() {
            var id int64; var proj string; var cr, dn *time.Time; var typ string
            if err := rows.Scan(&id, &proj, &cr, &dn, &typ); err != nil { return nil, err }
            doneIssues = append(doneIssues, rowDone{ID: id, Project: proj, Created: cr, Done: dn, Type: strings.ToLower(typ)})
        }
    }
    metrics["throughput_total"] = float64(len(doneIssues))
    if len(doneIssues) > 0 {
        countBy := map[string]int{}
        for _, d := range doneIssues { countBy[d.Type]++ }
        for typ, c := range countBy {
            key := fmt.Sprintf("throughput_%s", strings.ReplaceAll(typ, " ", "_"))
            metrics[key] = float64(c)
        }
    }

    // WIP count: not done
    var wipCount float64
    if err := r.db.Pool.QueryRow(ctx, `SELECT COALESCE(COUNT(*),0) FROM issues WHERE done_at IS NULL`).Scan(&wipCount); err != nil { return nil, err }
    metrics["wip_count"] = wipCount

    // Lead time avg (days) for items finished this week
    var leadAvgHours *float64
    if err := r.db.Pool.QueryRow(ctx, `SELECT AVG(EXTRACT(EPOCH FROM (done_at - created_at_jira))/3600.0)
        FROM issues WHERE done_at IS NOT NULL AND created_at_jira IS NOT NULL AND done_at >= $1 AND done_at < $2`, weekStart, weekEnd).Scan(&leadAvgHours); err == nil && leadAvgHours != nil {
        metrics["lead_time_days_avg"] = *leadAvgHours / 24.0
    } else { metrics["lead_time_days_avg"] = 0 }

    // Load status_map into memory for canonical mapping (board-agnostic fallback)
    canonicalMap := map[string]string{}
    {
        rows, err := r.db.Pool.Query(ctx, `SELECT DISTINCT jira_status, canonical_stage::text FROM status_map`)
        if err == nil {
            defer rows.Close()
            for rows.Next() { var js, st string; if err := rows.Scan(&js, &st); err == nil { canonicalMap[strings.ToLower(strings.TrimSpace(js))] = st } }
        }
    }
    toCanonical := func(status string) string {
        key := strings.ToLower(strings.TrimSpace(status))
        if v, ok := canonicalMap[key]; ok && v != "" { return v }
        // heuristic fallback
        switch {
        case key == "backlog": return "Backlog"
        case strings.Contains(key, "to do") || key == "todo": return "Queue"
        case strings.Contains(key, "in progress") || key == "doing": return "InProgress"
        case strings.Contains(key, "review") || strings.Contains(key, "ready4test"): return "Review"
        case strings.Contains(key, "test") || strings.Contains(key, "qa"): return "Test"
        case strings.Contains(key, "deploy") || strings.Contains(key, "release"): return "Deploy"
        case strings.Contains(key, "block") || key == "pending": return "Blocked"
        case strings.Contains(key, "done") || strings.Contains(key, "resolve"): return "Done"
        default: return "Queue"
        }
    }

    // Helper to load status and Flagged events for a set of issue IDs
    type statusEvent struct{ IssueID int64; Field string; From string; To string; At time.Time }
    loadEvents := func(ids []int64) ([]statusEvent, error) {
        if len(ids) == 0 { return nil, nil }
        // Prepare IN list
        // Use pgx Array for efficiency via ANY($1) requires int8[]; but simplest is OR chain when small.
        // For v1, fetch all and filter in Go.
        rows, err := r.db.Pool.Query(ctx, `SELECT issue_id, field, from_val, to_val, at FROM events WHERE field IN ('status','Flagged') ORDER BY issue_id, at`)
        if err != nil { return nil, err }
        defer rows.Close()
        set := map[int64]struct{}{}
        for _, id := range ids { set[id] = struct{}{} }
        var out []statusEvent
        for rows.Next() {
            var eid int64; var field string; var from, to *string; var at time.Time
            if err := rows.Scan(&eid, &field, &from, &to, &at); err != nil { return nil, err }
            if _, ok := set[eid]; !ok { continue }
            fv := ""; if from != nil { fv = *from }
            tv := ""; if to != nil { tv = *to }
            out = append(out, statusEvent{IssueID: eid, Field: field, From: fv, To: tv, At: at})
        }
        return out, nil
    }

    // Cycle time, queue time per stage, ping-pong, blocker dwell for issues done this week
    if len(doneIssues) > 0 {
        ids := make([]int64, 0, len(doneIssues))
        for _, d := range doneIssues { ids = append(ids, d.ID) }
        evts, err := loadEvents(ids)
        if err != nil { return nil, err }
        // Group by issue
        byIssue := map[int64][]statusEvent{}
        for _, e := range evts { byIssue[e.IssueID] = append(byIssue[e.IssueID], e) }

        var cycleHours []float64
        stageHours := map[string]float64{}
        flipsCount := 0
        blockersHours := 0.0
        flaggedHours := 0.0

        for _, d := range doneIssues {
            seq := byIssue[d.ID]
            sort.Slice(seq, func(i, j int) bool { return seq[i].At.Before(seq[j].At) })
            // Build timeline of canonical stages
            type seg struct{ Stage string; Start, End time.Time }
            var segments []seg
            var curStage string
            var curStart time.Time
            // Initialize current stage from first event's from or created_at
            // filter only status events for stage timeline
            var statusSeq []statusEvent
            for _, e := range seq { if strings.EqualFold(e.Field, "status") { statusSeq = append(statusSeq, e) } }
            if len(statusSeq) > 0 {
                curStage = toCanonical(statusSeq[0].From)
                if d.Created != nil { curStart = (*d.Created) } else { curStart = seq[0].At }
                for _, e := range statusSeq {
                    nextStage := toCanonical(e.To)
                    end := e.At
                    if !end.Before(curStart) { segments = append(segments, seg{Stage: curStage, Start: curStart, End: end}) }
                    curStage = nextStage
                    curStart = e.At
                }
            }
            // Close at done time
            if d.Done != nil && !d.Done.Before(curStart) {
                segments = append(segments, seg{Stage: curStage, Start: curStart, End: *d.Done})
            }
            // Accumulate stage hours and compute cycle
            firstInProgress := time.Time{}
            for _, s := range segments {
                durH := s.End.Sub(s.Start).Hours()
                if durH < 0 { continue }
                stageHours[s.Stage] += durH
                if s.Stage == "InProgress" && firstInProgress.IsZero() { firstInProgress = s.Start }
                if s.Stage == "Blocked" { blockersHours += durH }
            }
            // Accumulate flagged dwell from changelog toggles (field == 'Flagged')
            on := false
            var onStart time.Time
            for _, e := range seq {
                if !strings.EqualFold(e.Field, "Flagged") { continue }
                to := strings.TrimSpace(e.To)
                // Any non-empty indicates ON
                nonEmpty := to != "" && !strings.EqualFold(to, "none")
                if nonEmpty && !on {
                    on = true; onStart = e.At
                } else if !nonEmpty && on {
                    on = false
                    if d.Done != nil && e.At.After(*d.Done) { flaggedHours += d.Done.Sub(onStart).Hours() } else { flaggedHours += e.At.Sub(onStart).Hours() }
                }
            }
            if on && d.Done != nil { flaggedHours += d.Done.Sub(onStart).Hours() }
            if !firstInProgress.IsZero() && d.Done != nil {
                cycleHours = append(cycleHours, d.Done.Sub(firstInProgress).Hours())
            }
            // Ping-pong flips between InProgress and Review/Test
            flips := 0
            prev := ""
            for _, e := range statusSeq {
                to := toCanonical(e.To)
                if (prev == "InProgress" && (to == "Review" || to == "Test")) ||
                   ((prev == "Review" || prev == "Test") && to == "InProgress") {
                    flips++
                }
                prev = to
            }
            if flips >= 2 { flipsCount++ }
        }

        // Set metrics
        if len(cycleHours) > 0 {
            sum := 0.0
            for _, h := range cycleHours { sum += h }
            metrics["cycle_time_days_avg"] = (sum / float64(len(cycleHours))) / 24.0
        } else { metrics["cycle_time_days_avg"] = 0 }
        // Queue time per stage (avg per issue) in days for the week’s completed
        totalIssues := float64(len(doneIssues))
        for _, stage := range []string{"Backlog","Queue","InProgress","Review","Test","Deploy","Blocked"} {
            key := fmt.Sprintf("queue_time_days_%s", stage)
            metrics[key] = (stageHours[stage] / totalIssues) / 24.0
        }
        metrics["ping_pong_ge2_issues"] = float64(flipsCount)
        metrics["blocker_dwell_days_avg"] = (blockersHours / float64(maxi(1, len(doneIssues)))) / 24.0
        if flaggedHours > 0 { metrics["flagged_dwell_days_avg"] = (flaggedHours / float64(maxi(1, len(doneIssues)))) / 24.0 } else { metrics["flagged_dwell_days_avg"] = 0 }
    } else {
        metrics["cycle_time_days_avg"] = 0
        for _, stage := range []string{"Backlog","Queue","InProgress","Review","Test","Deploy","Blocked"} {
            metrics["queue_time_days_"+stage] = 0
        }
        metrics["ping_pong_ge2_issues"] = 0
        metrics["blocker_dwell_days_avg"] = 0
    }

    // WIP age for not-done issues that have entered InProgress
    {
        rows, err := r.db.Pool.Query(ctx, `SELECT id FROM issues WHERE done_at IS NULL`)
        if err != nil { return nil, err }
        defer rows.Close()
        var ids []int64
        for rows.Next() { var id int64; if err := rows.Scan(&id); err != nil { return nil, err }; ids = append(ids, id) }
        if len(ids) > 0 {
            evts, err := func() ([]statusEvent, error) {
                rows, err := r.db.Pool.Query(ctx, `SELECT issue_id, from_val, to_val, at FROM events WHERE field = 'status' ORDER BY issue_id, at`)
                if err != nil { return nil, err }
                defer rows.Close()
                set := map[int64]struct{}{}; for _, id := range ids { set[id] = struct{}{} }
                var out []statusEvent
                for rows.Next() { var eid int64; var from, to *string; var at time.Time; if err := rows.Scan(&eid, &from, &to, &at); err != nil { return nil, err }
                    if _, ok := set[eid]; !ok { continue }
                    fv := ""; if from != nil { fv = *from }; tv := ""; if to != nil { tv = *to }
                    out = append(out, statusEvent{IssueID: eid, From: fv, To: tv, At: at}) }
                return out, nil
            }()
            if err != nil { return nil, err }
            byIssue := map[int64][]statusEvent{}; for _, e := range evts { byIssue[e.IssueID] = append(byIssue[e.IssueID], e) }
            now := time.Now().UTC()
            count := 0
            sumHours := 0.0
            for _, id := range ids {
                seq := byIssue[id]
                sort.Slice(seq, func(i, j int) bool { return seq[i].At.Before(seq[j].At) })
                entered := time.Time{}
                cur := ""
                for _, e := range seq {
                    to := toCanonical(e.To)
                    if cur != "InProgress" && to == "InProgress" { entered = e.At; break }
                    cur = to
                }
                if !entered.IsZero() {
                    sumHours += now.Sub(entered).Hours()
                    count++
                }
            }
            if count > 0 { metrics["wip_age_days_avg"] = (sumHours / float64(count)) / 24.0 } else { metrics["wip_age_days_avg"] = 0 }
        } else { metrics["wip_age_days_avg"] = 0 }
    }

    return metrics, nil
}

func maxi(a, b int) int { if a > b { return a } ; return b }

// Persist metrics for a week and squad key
func (r *Repository) BulkInsertMetrics(ctx context.Context, weekStart time.Time, squad string, metrics map[string]float64) error {
    if len(metrics) == 0 { return nil }
    batch := &pgx.Batch{}
    const q = `INSERT INTO metrics_weekly(week_start, squad, kpi, value) VALUES($1,$2,$3,$4)
        ON CONFLICT (week_start, squad, kpi) DO UPDATE SET value=EXCLUDED.value`
    for k, v := range metrics { batch.Queue(q, weekStart, squad, k, v) }
    br := r.db.Pool.SendBatch(ctx, batch)
    defer br.Close()
    for range metrics { if _, err := br.Exec(); err != nil { return err } }
    return nil
}

// Load existing metrics for a week/squad
func (r *Repository) GetWeeklyMetrics(ctx context.Context, weekStart time.Time, squad string) (map[string]float64, error) {
    rows, err := r.db.Pool.Query(ctx, `SELECT kpi, value FROM metrics_weekly WHERE week_start=$1 AND squad=$2`, weekStart, squad)
    if err != nil { return nil, err }
    defer rows.Close()
    out := map[string]float64{}
    for rows.Next() { var k string; var v float64; if err := rows.Scan(&k, &v); err != nil { return nil, err }; out[k] = v }
    return out, nil
}

// ComputeWeeklyProjectCounts returns minimal metrics per project for the given week
func (r *Repository) ComputeWeeklyProjectCounts(ctx context.Context, weekStart time.Time) (map[string]map[string]float64, error) {
    weekEnd := weekStart.Add(7 * 24 * time.Hour)
    out := map[string]map[string]float64{}
    // throughput per project
    rows, err := r.db.Pool.Query(ctx, `SELECT COALESCE(project,''), COUNT(*) FROM issues WHERE done_at >= $1 AND done_at < $2 GROUP BY 1`, weekStart, weekEnd)
    if err != nil { return nil, err }
    defer rows.Close()
    for rows.Next() { var proj string; var c float64; if err := rows.Scan(&proj, &c); err != nil { return nil, err }
        if _, ok := out[proj]; !ok { out[proj] = map[string]float64{} }
        out[proj]["throughput_total"] = c
    }
    // WIP count per project
    rows2, err := r.db.Pool.Query(ctx, `SELECT COALESCE(project,''), COUNT(*) FROM issues WHERE done_at IS NULL GROUP BY 1`)
    if err != nil { return nil, err }
    defer rows2.Close()
    for rows2.Next() { var proj string; var c float64; if err := rows2.Scan(&proj, &c); err != nil { return nil, err }
        if _, ok := out[proj]; !ok { out[proj] = map[string]float64{} }
        out[proj]["wip_count"] = c
    }
    return out, nil
}
func (r *Repository) SaveFinding(ctx context.Context, f domain.Finding) error { return nil }

// Job runs
func (r *Repository) StartJobRun(ctx context.Context, boardsJSON string) (int64, error) {
	const q = `INSERT INTO job_runs(started_at, boards, success) VALUES(now(), $1, false) RETURNING id`
	var id int64
	if err := r.db.Pool.QueryRow(ctx, q, boardsJSON).Scan(&id); err != nil { return 0, err }
	return id, nil
}

func (r *Repository) FinishJobRun(ctx context.Context, id int64, issuesScanned, issuesLLM, tokensUsed int, success bool, errStr string) error {
	const q = `UPDATE job_runs SET finished_at=now(), issues_scanned=$2, issues_llm=$3, tokens_used=$4, success=$5, error=$6 WHERE id=$1`
	_, err := r.db.Pool.Exec(ctx, q, id, issuesScanned, issuesLLM, tokensUsed, success, errStr)
	return err
}

type LastRun struct {
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at"`
	Boards       string `json:"boards"`
	IssuesScanned int `json:"issues_scanned"`
	IssuesLLM    int `json:"issues_llm"`
	TokensUsed   int `json:"tokens_used"`
	Success      bool `json:"success"`
	Error        string `json:"error"`
}

func (r *Repository) GetLastRun(ctx context.Context) (*LastRun, error) {
    const q = `SELECT started_at, finished_at, boards::text,
        coalesce(issues_scanned,0), coalesce(issues_llm,0), coalesce(tokens_used,0),
        coalesce(success,false), coalesce(error,'')
		FROM job_runs ORDER BY id DESC LIMIT 1`
	row := r.db.Pool.QueryRow(ctx, q)
	lr := &LastRun{}
	if err := row.Scan(&lr.StartedAt, &lr.FinishedAt, &lr.Boards, &lr.IssuesScanned, &lr.IssuesLLM, &lr.TokensUsed, &lr.Success, &lr.Error); err != nil {
		return nil, err
	}
	return lr, nil
}

// SeedStatusMap seeds default mappings for a board
func (r *Repository) SeedStatusMap(ctx context.Context, boardID int64) error {
	if boardID <= 0 { return fmt.Errorf("invalid board id") }
	pairs := [][2]string{
		{"Backlog","Backlog"},
		{"To Do","Queue"},
		{"ToDo","Queue"},
		{"In Progress","InProgress"},
		{"Pending","Blocked"},
		{"Ready4Test","Review"},
		{"Under Review","Review"},
		{"In Test","Test"},
		{"Ready to Deploy","Deploy"},
		{"Done","Done"},
	}
	for _, p := range pairs {
        _, err := r.db.Pool.Exec(ctx, `INSERT INTO status_map(board_id, jira_status, canonical_stage) VALUES($1,$2,$3)
            ON CONFLICT (board_id, jira_status) DO NOTHING`, boardID, p[0], p[1])
		if err != nil { return err }
	}
	return nil
}

// ResolveCanonical returns canonical stage using status_map when available, otherwise heuristic fallback
func (r *Repository) ResolveCanonical(ctx context.Context, boardID int64, jiraStatus string) (string, error) {
    if boardID > 0 {
        var stage *string
        if err := r.db.Pool.QueryRow(ctx, `SELECT canonical_stage::text FROM status_map WHERE board_id=$1 AND jira_status=$2`, boardID, jiraStatus).Scan(&stage); err == nil && stage != nil {
            return *stage, nil
        }
    }
    // fallback heuristic similar to ComputeWeeklyMetrics
    s := strings.ToLower(strings.TrimSpace(jiraStatus))
    switch {
    case s == "backlog":
        return "Backlog", nil
    case strings.Contains(s, "to do") || s == "todo":
        return "Queue", nil
    case strings.Contains(s, "in progress") || s == "doing":
        return "InProgress", nil
    case strings.Contains(s, "review") || strings.Contains(s, "ready4test"):
        return "Review", nil
    case strings.Contains(s, "test") || strings.Contains(s, "qa"):
        return "Test", nil
    case strings.Contains(s, "deploy") || strings.Contains(s, "release"):
        return "Deploy", nil
    case strings.Contains(s, "block") || s == "pending":
        return "Blocked", nil
    case strings.Contains(s, "done") || strings.Contains(s, "resolve"):
        return "Done", nil
    default:
        return "Queue", nil
    }
}

// ---- Anomaly prefilter helpers (G) ----
type IssueCandidate struct {
    ID        int64
    Key       string
    Type      string
    Priority  string
    Status    string
    UpdatedAt *time.Time
    DoneAt    *time.Time
}

// ListIssueCandidates returns issues touched since the given time
func (r *Repository) ListIssueCandidates(ctx context.Context, since time.Time) ([]IssueCandidate, error) {
    rows, err := r.db.Pool.Query(ctx, `
        SELECT id, key, COALESCE(type,''), COALESCE(priority,''), COALESCE(status_last,''),
               updated_at_jira, done_at
        FROM issues
        WHERE (updated_at_jira IS NOT NULL AND updated_at_jira >= $1)
           OR (done_at IS NOT NULL AND done_at >= $1)
    `, since)
    if err != nil { return nil, err }
    defer rows.Close()
    var out []IssueCandidate
    for rows.Next() {
        var ic IssueCandidate
        if err := rows.Scan(&ic.ID, &ic.Key, &ic.Type, &ic.Priority, &ic.Status, &ic.UpdatedAt, &ic.DoneAt); err != nil { return nil, err }
        out = append(out, ic)
    }
    return out, nil
}

type SimpleEvent struct { IssueID int64; Field string; From string; To string; At time.Time }

func (r *Repository) LoadStatusAndFlagEvents(ctx context.Context, ids []int64) ([]SimpleEvent, error) {
    if len(ids) == 0 { return nil, nil }
    rows, err := r.db.Pool.Query(ctx, `
        SELECT issue_id, field, COALESCE(from_val,''), COALESCE(to_val,''), at
        FROM events
        WHERE issue_id = ANY($1) AND field IN ('status','Flagged')
        ORDER BY issue_id, at
    `, ids)
    if err != nil { return nil, err }
    defer rows.Close()
    var out []SimpleEvent
    for rows.Next() {
        var e SimpleEvent
        if err := rows.Scan(&e.IssueID, &e.Field, &e.From, &e.To, &e.At); err != nil { return nil, err }
        out = append(out, e)
    }
    return out, nil
}

func (r *Repository) CountCommentsAndHistory(ctx context.Context, ids []int64) (map[int64]int, map[int64]int, error) {
    if len(ids) == 0 { return map[int64]int{}, map[int64]int{}, nil }
    comments := map[int64]int{}
    histories := map[int64]int{}
    // comments
    if rows, err := r.db.Pool.Query(ctx, `SELECT issue_id, COUNT(*) FROM comments WHERE issue_id = ANY($1) GROUP BY 1`, ids); err == nil {
        defer rows.Close()
        for rows.Next() { var id int64; var c int; if err := rows.Scan(&id, &c); err != nil { return nil, nil, err }; comments[id] = c }
    } else { return nil, nil, err }
    // history events
    if rows, err := r.db.Pool.Query(ctx, `SELECT issue_id, COUNT(*) FROM events WHERE issue_id = ANY($1) AND kind='changelog' GROUP BY 1`, ids); err == nil {
        defer rows.Close()
        for rows.Next() { var id int64; var c int; if err := rows.Scan(&id, &c); err != nil { return nil, nil, err }; histories[id] = c }
    } else { return nil, nil, err }
    return comments, histories, nil
}

// ---- Data access for LLM ----
func (r *Repository) GetIssueByKey(ctx context.Context, key string) (*domain.Issue, error) {
    const q = `SELECT id, key, COALESCE(project,''), COALESCE(type,''), COALESCE(priority,''),
        COALESCE(assignee,''), COALESCE(reporter,''), COALESCE(status_last,''),
        created_at_jira, updated_at_jira, done_at, points_estimate, COALESCE(squad,''),
        due_at, COALESCE(labels, '{}'), COALESCE(subteam,''), COALESCE(service,''), COALESCE(epic_key,'')
        FROM issues WHERE key=$1`
    row := r.db.Pool.QueryRow(ctx, q, key)
    var i domain.Issue
    if err := row.Scan(&i.ID, &i.Key, &i.Project, &i.Type, &i.Priority, &i.Assignee, &i.Reporter, &i.StatusLast,
        &i.CreatedAtJira, &i.UpdatedAtJira, &i.DoneAt, &i.PointsEstimate, &i.Squad,
        &i.DueAt, &i.Labels, &i.Subteam, &i.Service, &i.EpicKey); err != nil { return nil, err }
    return &i, nil
}

func (r *Repository) GetCommentsByIssueID(ctx context.Context, issueID int64, limit int) ([]domain.Comment, error) {
    if limit <= 0 { limit = 50 }
    rows, err := r.db.Pool.Query(ctx, `SELECT id, issue_id, COALESCE(ext_id,''), COALESCE(author,''), at, COALESCE(body,'')
        FROM comments WHERE issue_id=$1 ORDER BY at DESC LIMIT $2`, issueID, limit)
    if err != nil { return nil, err }
    defer rows.Close()
    var out []domain.Comment
    for rows.Next() {
        var c domain.Comment
        if err := rows.Scan(&c.ID, &c.IssueID, &c.ExtID, &c.Author, &c.At, &c.Body); err != nil { return nil, err }
        out = append(out, c)
    }
    // reverse to chronological
    for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 { out[i], out[j] = out[j], out[i] }
    return out, nil
}

func (r *Repository) GetEventsByIssueID(ctx context.Context, issueID int64, limit int) ([]domain.Event, error) {
    if limit <= 0 { limit = 100 }
    rows, err := r.db.Pool.Query(ctx, `SELECT id, issue_id, kind, COALESCE(field,''), COALESCE(from_val,''), COALESCE(to_val,''), at
        FROM events WHERE issue_id=$1 AND kind='changelog' ORDER BY at DESC LIMIT $2`, issueID, limit)
    if err != nil { return nil, err }
    defer rows.Close()
    var out []domain.Event
    for rows.Next() {
        var e domain.Event
        if err := rows.Scan(&e.ID, &e.IssueID, &e.Kind, &e.Field, &e.FromVal, &e.ToVal, &e.At); err != nil { return nil, err }
        out = append(out, e)
    }
    for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 { out[i], out[j] = out[j], out[i] }
    return out, nil
}


