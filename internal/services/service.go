/* Copyright (c) 2025 Hamed Shams <https://hamedshams.com>
 * SPDX-License-Identifier: BSD-3-Clause */
package services

import (
    "context"
    "encoding/json"
    "fmt"
    "sort"
    "regexp"
    "sync"
    "strings"
    "time"

    "github.com/HamedShams/agile-pulse/internal/config"
    "github.com/HamedShams/agile-pulse/internal/domain"
    "github.com/HamedShams/agile-pulse/internal/repo"
    "github.com/rs/zerolog"
)

type JiraClient interface {
    Search(ctx context.Context, jql string, startAt, max int) (any, error)
    Comments(ctx context.Context, key string, startAt, max int) (any, error)
    Changelog(ctx context.Context, key string, startAt, max int) (any, error)
    Worklogs(ctx context.Context, key string, startAt, max int) (any, error)
    Boards(ctx context.Context, startAt, max int) (any, error)
    BoardIssues(ctx context.Context, boardID int64, startAt, max int, jql string) (any, error)
    ResolveBoardsByNames(ctx context.Context, names []string) ([]int64, error)
    Fields(ctx context.Context) (any, error)
    Issue(ctx context.Context, key string, expandChangelog bool) (any, error)
}

type LLM interface {
    ExtractIssue(ctx context.Context, payload any) (map[string]any, error)
    Summarize(ctx context.Context, kpis map[string]float64, findings []map[string]any) (string, error)
}

type Notifier interface {
    SendMessage(ctx context.Context, chatID int64, text string) error
    SendMessagePlain(ctx context.Context, chatID int64, text string) error
    SendMarkdownV2(ctx context.Context, chatID int64, text string) error
}

type Service struct {
    cfg  config.Config
    log  zerolog.Logger
    repo *repo.Repository
    jira JiraClient
    llm  LLM
    tg   Notifier
    boardIDs []int64
    flaggedFields []string
    customFields map[string]string
}

func New(cfg config.Config, log zerolog.Logger, r *repo.Repository, jira JiraClient, llm LLM, tg Notifier) *Service {
    return &Service{cfg: cfg, log: log, repo: r, jira: jira, llm: llm, tg: tg}
}

// prefilterIssues selects top risky issues within token and count limits for the last N days
func (s *Service) prefilterIssues(ctx context.Context, sinceDays int) ([]string, int) {
    since := time.Now().UTC().Add(time.Duration(-24*sinceDays) * time.Hour)
    cands, err := s.repo.ListIssueCandidates(ctx, since)
    if err != nil { s.log.Error().Err(err).Msg("prefilter: list candidates failed"); return nil, 0 }
    ids := make([]int64, 0, len(cands))
    for _, c := range cands { ids = append(ids, c.ID) }
    comments, history, err := s.repo.CountCommentsAndHistory(ctx, ids)
    if err != nil { s.log.Error().Err(err).Msg("prefilter: count comments/history failed"); return nil, 0 }
    evts, err := s.repo.LoadStatusAndFlagEvents(ctx, ids)
    if err != nil { s.log.Error().Err(err).Msg("prefilter: load events failed") }
    byIssue := map[int64][]repo.SimpleEvent{}
    for _, e := range evts { byIssue[e.IssueID] = append(byIssue[e.IssueID], e) }
    // Compute risk score
    type scored struct { Key string; Score float64; EstTokens int }
    var list []scored
    now := time.Now().UTC()
    for _, c := range cands {
        // recency in days
        rec := 0.0
        if c.UpdatedAt != nil { rec = now.Sub(*c.UpdatedAt).Hours()/24.0 } else if c.DoneAt != nil { rec = now.Sub(*c.DoneAt).Hours()/24.0 }
        if rec < 0 { rec = 0 }
        recencyScore := 1.0 / (1.0 + rec) // 0..1
        pri := strings.ToLower(c.Priority)
        priScore := 0.0
        switch pri {
        case "blocker", "highest": priScore = 1.0
        case "critical", "high": priScore = 0.7
        case "medium": priScore = 0.4
        default: priScore = 0.2
        }
        typeScore := 0.3
        if strings.Contains(strings.ToLower(c.Type), "bug") { typeScore = 0.8 }
        // flagged signal in window
        flaggedScore := 0.0
        if seq, ok := byIssue[c.ID]; ok {
            // detect any ON within window
            on := false
            onWindow := false
            for _, e := range seq {
                if !strings.EqualFold(e.Field, "Flagged") { continue }
                nonEmpty := strings.TrimSpace(e.To) != "" && !strings.EqualFold(strings.TrimSpace(e.To), "none")
                if nonEmpty { on = true; if e.At.After(since) { onWindow = true } } else { on = false }
            }
            if on || onWindow { flaggedScore = 1.0 } // strong signal
        }
        // WIP age score for not-done items: longer in progress => higher
        wipAgeScore := 0.0
        if c.DoneAt == nil {
            if seq, ok := byIssue[c.ID]; ok {
                inProgEnter := time.Time{}
                cur := ""
                for _, e := range seq {
                    if !strings.EqualFold(e.Field, "status") { continue }
                    to := strings.ToLower(strings.TrimSpace(e.To))
                    canon := to
                    if strings.Contains(to, "in progress") || to == "doing" { canon = "InProgress" }
                    if cur != "InProgress" && canon == "InProgress" { inProgEnter = e.At }
                    cur = canon
                }
                if !inProgEnter.IsZero() {
                    days := now.Sub(inProgEnter).Hours()/24.0
                    if days < 0 { days = 0 }
                    if days > 0 { wipAgeScore = days / 14.0; if wipAgeScore > 1 { wipAgeScore = 1 } }
                }
            }
        }
        comm := float64(comments[c.ID])
        hist := float64(history[c.ID])
        densityScore := 1.0 - 1.0/(1.0+0.1*(comm+hist))
        score := 0.30*recencyScore + 0.25*priScore + 0.15*typeScore + 0.15*densityScore + 0.10*flaggedScore + 0.05*wipAgeScore
        // rough token estimate per issue: base + per comment/history
        estTok := 400 + int(comm)*120 + int(hist)*80
        list = append(list, scored{Key: c.Key, Score: score, EstTokens: estTok})
    }
    sort.Slice(list, func(i, j int) bool { if list[i].Score == list[j].Score { return list[i].EstTokens < list[j].EstTokens }; return list[i].Score > list[j].Score })
    maxIssues := s.cfg.LLMMaxIssues
    if maxIssues <= 0 { maxIssues = 60 }
    budget := s.cfg.LLMBudgetTokens
    if budget <= 0 { budget = 300000 }
    sel := []string{}
    used := 0
    for _, it := range list {
        if len(sel) >= maxIssues { break }
        if used+it.EstTokens > budget { continue }
        sel = append(sel, it.Key)
        used += it.EstTokens
    }
    return sel, used
}

// prepareIssueForLLM gathers issue + recent comments/history
func (s *Service) prepareIssueForLLM(ctx context.Context, key string) map[string]any {
    out := map[string]any{"key": key}
    iss, err := s.repo.GetIssueByKey(ctx, key)
    if err == nil && iss != nil {
        out["issue"] = iss
        if cs, err := s.repo.GetCommentsByIssueID(ctx, iss.ID, 100); err == nil { out["comments"] = cs }
        if ev, err := s.repo.GetEventsByIssueID(ctx, iss.ID, 500); err == nil { out["history"] = ev }
    }
    return out
}

// redactPII removes obvious PII/secrets and aliases authors
func redactPII(payload map[string]any) map[string]any {
    emailRe := regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+`)
    phoneRe := regexp.MustCompile(`\b\+?\d[\d\-\s]{7,}\b`)
    urlRe := regexp.MustCompile(`https?://[^\s]+`)
    tokenRe := regexp.MustCompile(`(?i)\b(?:token|secret|password|apikey|api_key|bearer)[:=\s]+[A-Za-z0-9\-\._~+/]{8,}\b`)
    jiraUserRe := regexp.MustCompile(`\bJIRAUSER\d+\b`)

    scrub := func(s string) string {
        s = strings.ReplaceAll(s, "\r\n", "\n")
        s = emailRe.ReplaceAllString(s, "<email>")
        s = phoneRe.ReplaceAllString(s, "<phone>")
        s = urlRe.ReplaceAllString(s, "<url>")
        s = tokenRe.ReplaceAllString(s, "<secret>")
        s = jiraUserRe.ReplaceAllString(s, "<user>")
        return s
    }
    // Build alias map from comments authors
    alias := map[string]string{}
    next := 1
    if cm, ok := payload["comments"].([]domain.Comment); ok {
        for _, c := range cm {
            a := strings.TrimSpace(c.Author)
            if a == "" { continue }
            if _, ok := alias[a]; !ok { alias[a] = fmt.Sprintf("user%02d", next); next++ }
        }
        // Replace author names in comment bodies (best-effort word-boundary)
        nameRes := make([]*regexp.Regexp, 0, len(alias))
        names := make([]string, 0, len(alias))
        for name := range alias {
            // escape regex special chars
            esc := regexp.QuoteMeta(name)
            nameRes = append(nameRes, regexp.MustCompile(`(?i)\b`+esc+`\b`))
            names = append(names, name)
        }
        for i := range cm {
            a := cm[i].Author
            if v, ok := alias[a]; ok { cm[i].Author = v }
            body := scrub(cm[i].Body)
            for idx, re := range nameRes { body = re.ReplaceAllString(body, alias[names[idx]]) }
            cm[i].Body = body
        }
        payload["comments"] = cm
    }
    if hv, ok := payload["history"].([]domain.Event); ok {
        for i := range hv {
            hv[i].FromVal = scrub(hv[i].FromVal)
            hv[i].ToVal = scrub(hv[i].ToVal)
        }
        payload["history"] = hv
    }
    return payload
}

func (s *Service) RunWeeklyDigest(ctx context.Context) error {
    // resolve boards and seed status map
    if err := s.ensureBoards(ctx); err != nil { s.log.Error().Err(err).Msg("ensure boards failed") }
    // discover flagged custom field ids for E)
    if err := s.ensureFlaggedFields(ctx); err != nil { s.log.Error().Err(err).Msg("discover flagged field failed") }
    _ = s.ensureCustomFields(ctx)

    // start job run
    boardsJSON, _ := json.Marshal(s.boardIDs)
    runID, err := s.repo.StartJobRun(ctx, string(boardsJSON))
    if err != nil { s.log.Error().Err(err).Msg("start job run failed") }
    s.log.Info().Msg("WeeklyDigest: start")
    weekStart := weekStart(time.Now())
    // Run ETL for last 7 days (skeleton) and always finalize job_run
    var issuesScanned int
    var llmSelected int
    var llmTokens int
    var etlErr error
    defer func(){
        if runID != 0 {
            _ = s.repo.FinishJobRun(ctx, runID, issuesScanned, llmSelected, llmTokens, etlErr == nil, fmt.Sprintf("%v", etlErr))
        }
    }()
    issuesScanned, etlErr = s.runETL(ctx, 7)
    metrics, _ := s.repo.ComputeWeeklyMetrics(ctx, weekStart, "")
    // persist metrics and deltas vs previous week
    _ = s.repo.BulkInsertMetrics(ctx, weekStart, "", metrics)
    prev, _ := s.repo.GetWeeklyMetrics(ctx, weekStart.Add(-7*24*time.Hour), "")
    if len(prev) > 0 {
        deltas := map[string]float64{}
        for k, v := range metrics {
            if pv, ok := prev[k]; ok { deltas[k+"_delta"] = v - pv }
        }
        if len(deltas) > 0 { _ = s.repo.BulkInsertMetrics(ctx, weekStart, "", deltas) }
    }
    // Anomaly prefilter (G): select top risky issues within token budget
    if etlErr == nil {
        sel, tokens := s.prefilterIssues(ctx, 7)
        llmSelected = len(sel)
        llmTokens = tokens
        s.log.Info().Int("llm_issues", llmSelected).Int("llm_tokens_est", llmTokens).Strs("llm_keys", sel).Msg("prefilter selected for LLM")
        // I) LLM pipeline (concurrent extracts)
        var findings []map[string]any
        if s.llm != nil && strings.TrimSpace(s.cfg.OpenAIKey) != "" && llmSelected > 0 {
            type result struct{ f map[string]any; err error }
            jobs := make(chan string)
            results := make(chan result)
            workerCount := s.cfg.WorkersLLM
            if workerCount <= 0 { workerCount = 3 }
            for w := 0; w < workerCount; w++ {
                go func() {
                    for key := range jobs {
                        fi := s.prepareIssueForLLM(ctx, key)
                        red := redactPII(fi)
                        f, err := s.llm.ExtractIssue(ctx, red)
                        results <- result{f: f, err: err}
                    }
                }()
            }
            go func(){ for _, k := range sel { jobs <- k }; close(jobs) }()
            for i := 0; i < len(sel); i++ { r := <-results; if r.err == nil && r.f != nil { findings = append(findings, r.f) } }
        }
        // J) Digest rendering and delivery
        digest := s.renderDigest(metrics, findings)
        if digest != "" {
            parts := chunkText(digest, 3800)
            for _, chat := range s.cfg.TelegramChatIDs { for _, p := range parts { _ = s.tg.SendMarkdownV2(ctx, chat, p) } }
        }
    }
    // persist minimal per-project metrics (throughput_total, wip_count)
    if proj, err := s.repo.ComputeWeeklyProjectCounts(ctx, weekStart); err == nil {
        for p, mm := range proj {
            _ = s.repo.BulkInsertMetrics(ctx, weekStart, p, mm)
            if prevp, err2 := s.repo.GetWeeklyMetrics(ctx, weekStart.Add(-7*24*time.Hour), p); err2 == nil && len(prevp) > 0 {
                del := map[string]float64{}
                for k, v := range mm { if pv, ok := prevp[k]; ok { del[k+"_delta"] = v - pv } }
                if len(del) > 0 { _ = s.repo.BulkInsertMetrics(ctx, weekStart, p, del) }
            }
        }
    }
    // brief textual summary for now
    tp := int(metrics["throughput_total"])
    wip := int(metrics["wip_count"])
    lead := metrics["lead_time_days_avg"]
    cycle := metrics["cycle_time_days_avg"]
    td := metrics["throughput_total_delta"]
    summary := fmt.Sprintf("*Agile Pulse*\nWeekly digest (stub).\nThroughput: %d (Δ%.0f)\nWIP: %d\nLead avg: %.1fd\nCycle avg: %.1fd", tp, td, wip, lead, cycle)
    // Send by numeric chat IDs if present
    for _, chat := range s.cfg.TelegramChatIDs {
        if err := s.tg.SendMessage(ctx, chat, summary); err != nil {
            s.log.Error().Err(err).Int64("chat", chat).Msg("telegram send failed")
        }
    }
    // If no numeric IDs, try usernames via resolver if available
    type usernameResolver interface{ ResolveUsername(ctx context.Context, username string) (int64, error) }
    if len(s.cfg.TelegramChatIDs) == 0 && len(s.cfg.TelegramChatUsernames) > 0 {
        if r, ok := s.tg.(usernameResolver); ok {
            for _, u := range s.cfg.TelegramChatUsernames {
                id, err := r.ResolveUsername(ctx, u)
                if err != nil { s.log.Error().Err(err).Str("username", u).Msg("resolve username failed"); continue }
                if err := s.tg.SendMessage(ctx, id, summary); err != nil {
                    s.log.Error().Err(err).Str("username", u).Int64("chat", id).Msg("telegram send failed")
                }
            }
        } else {
            s.log.Error().Msg("telegram client does not support username resolution; set TELEGRAM_CHAT_IDS")
        }
    }
    s.log.Info().Time("weekStart", weekStart).Msg("WeeklyDigest: done")
    return nil
}

// RunOnDemandDigest generates a digest for the past N days and sends it to the requester chat.
func (s *Service) RunOnDemandDigest(ctx context.Context, chatID int64, sinceDays int) error {
    if chatID == 0 { return nil }
    if sinceDays <= 0 { sinceDays = 7 }
    summary := "*Agile Pulse*\nOn-demand report (stub).\nRange: last " + time.Duration(sinceDays*24).String()
    return s.tg.SendMessage(ctx, chatID, summary)
}

// SendHelp replies with bot capabilities and commands.
func (s *Service) SendHelp(ctx context.Context, chatID int64) error {
    if chatID == 0 { return nil }
    // Minimal MarkdownV2 help (escape special characters)
    esc := func(s string) string {
        repl := []string{"_","\\_","*","\\*","[","\\[","]","\\]","(","\\(",")","\\)","~","\\~","`","\\`",">","\\>","#","\\#","+","\\+","-","\\-","=","\\=","|","\\|","{","\\{","}","\\}",".","\\.","!","\\!"}
        for i := 0; i < len(repl); i+=2 { s = strings.ReplaceAll(s, repl[i], repl[i+1]) }
        return s
    }
    help := esc("Agile Pulse Bot") + "\n" +
        esc("Weekly Jira insights with metrics + LLM-based findings.") + "\n\n" +
        esc("Commands:") + "\n" +
        esc("- /report 7d — On-demand report for the last 7 days") + "\n" +
        esc("- /report 30d — On-demand report for the last 30 days") + "\n" +
        esc("Setup: Admin configures Jira, Telegram chat IDs, and schedule.")
    return s.tg.SendMarkdownV2(ctx, chatID, help)
}

// TestJiraFetch fetches a small sample from Jira and sends a structured preview to Telegram.
func (s *Service) TestJiraFetch(ctx context.Context) error {
    // Fetch only a specific board: "ALPHA BOARD" within SNAPPDR, last 2 days
    cutoff := time.Now().UTC().Add(-48 * time.Hour)
    boardName := "ALPHA BOARD"
    projectKey := "SNAPPDR"
    // Robust board discovery: list all boards and pick the one whose name contains boardName (case-insensitive), prefer project match
    var boardID int64
    var allNames []string
    lowerNeedle := strings.ToLower(boardName)
    start := 0
    for {
        page, err := s.jira.Boards(ctx, start, 50)
        if err != nil { break }
        pm, _ := page.(map[string]any)
        vals, _ := pm["values"].([]any)
        if len(vals) == 0 { break }
        for _, v0 := range vals {
            b, _ := v0.(map[string]any)
            if b == nil { continue }
            name := toStrAny(b["name"])
            allNames = append(allNames, name)
            match := strings.Contains(strings.ToLower(name), lowerNeedle)
            prefer := false
            if loc, ok := b["location"].(map[string]any); ok {
                if toStrAny(loc["projectKey"]) == projectKey { prefer = true }
            }
            if match {
                var id int64
                switch vv := b["id"].(type) { case float64: id = int64(vv); case int64: id = vv }
                if id > 0 {
                    if boardID == 0 || prefer { boardID = id }
                }
            }
        }
        if len(vals) < 50 { break }
        start += 50
    }
    if boardID == 0 {
        s.log.Error().Str("board", boardName).Strs("seen", allNames).Msg("jira test: board not found by contains")
        // Also notify via Telegram for easier debugging
        for _, chat := range s.cfg.TelegramChatIDs {
            _ = s.tg.SendMessagePlain(ctx, chat, "Jira test: board not found: "+boardName+". Seen boards: "+strings.Join(allNames, ", "))
        }
        return fmt.Errorf("board %s not found", boardName)
    }
    jql := fmt.Sprintf("project=%s AND updated >= -2d", projectKey)

    type simpleComment struct { ID string `json:"id"`; Author string `json:"author"`; Created string `json:"created"`; Body string `json:"body"` }
    type simpleChange struct { At string `json:"at"`; Field string `json:"field"`; From string `json:"from"`; To string `json:"to"` }
    type simpleWork struct { ID string `json:"id"`; Author string `json:"author"`; Started string `json:"started"`; Seconds int `json:"seconds"` }

    var messages []string
    // discover custom fields used for display in test output
    _ = s.ensureCustomFields(ctx)

    startAt := 0
    for {
        res, err := s.jira.BoardIssues(ctx, boardID, startAt, 50, jql)
        if err != nil { return err }
        m, _ := res.(map[string]any)
        arr, _ := m["issues"].([]any)
        if len(arr) == 0 { break }
        for _, it := range arr {
            im, _ := it.(map[string]any)
            if im == nil { continue }
            key := toStrAny(im["key"])
            fields, _ := im["fields"].(map[string]any)
            summary := toStrAny(fields["summary"])
            status := ""; if st, ok := fields["status"].(map[string]any); ok { status = toStrAny(st["name"]) }
            updated := toStrAny(fields["updated"]) 
            created := toStrAny(fields["created"]) 
            priority := ""; if pr, ok := fields["priority"].(map[string]any); ok { priority = toStrAny(pr["name"]) }
            assignee := ""; assigneeKey := ""; if as, ok := fields["assignee"].(map[string]any); ok { assignee = toStrAny(as["displayName"]); assigneeKey = toStrAny(as["accountId"]); if assigneeKey == "" { assigneeKey = toStrAny(as["name"]) } }
            reporter := ""; reporterKey := ""; if rp, ok := fields["reporter"].(map[string]any); ok { reporter = toStrAny(rp["displayName"]); reporterKey = toStrAny(rp["accountId"]); if reporterKey == "" { reporterKey = toStrAny(rp["name"]) } }
            dueDate := toStrAny(fields["duedate"]) 
            // labels
            var labels []string
            if lv, ok := fields["labels"].([]any); ok { for _, x := range lv { if s, ok := x.(string); ok { labels = append(labels, s) } } }
            // story points
            var estimate any
            if k := s.customFields["Story Points"]; k != "" { if v, ok := fields[k]; ok { estimate = v } }
            if estimate == nil { if v, ok := fields["customfield_10016"]; ok { estimate = v } }
            // optional custom fields
            subteam := ""
            if k := s.customFields["Subteam"]; k != "" { subteam = optionToString(fields[k]) } else { subteam = optionToString(fields["subteam"]) }
            service := ""
            if k := s.customFields["Service"]; k != "" { service = optionToString(fields[k]) } else { service = optionToString(fields["service"]) }
            if service == "" {
                // Fallback: components array often used as Service/Team in projects
                if comps, ok := fields["components"].([]any); ok {
                    names := make([]string, 0, len(comps))
                    for _, c0 := range comps {
                        if cm, _ := c0.(map[string]any); cm != nil { n := toStrAny(cm["name"]); if n != "" { names = append(names, n) } }
                    }
                    if len(names) > 0 { service = strings.Join(names, ", ") }
                }
            }
            if service == "" {
                // Last resort: scan fields for any key containing 'service'
                for k, v := range fields {
                    if strings.Contains(strings.ToLower(k), "service") {
                        val := optionToString(v)
                        if val != "" { service = val; break }
                    }
                }
            }
            epicKey := ""
            if k := s.customFields["Epic Link"]; k != "" { epicKey = toStrAny(fields[k]) }
            if epicKey == "" { if ep, ok := fields["epic"].(map[string]any); ok { epicKey = toStrAny(ep["key"]) } }

            // comments: include all (robust pagination using response metadata)
            var comments []simpleComment
            cStart := 0
            for {
                cresAny, _ := s.jira.Comments(ctx, key, cStart, 100)
                cm, _ := cresAny.(map[string]any)
                carr, _ := cm["comments"].([]any)
                if len(carr) == 0 { break }
                for _, c0 := range carr {
                    cmi, _ := c0.(map[string]any)
                    if cmi == nil { continue }
                    created := parseTimeUTC(cmi["created"]) 
                    id := toStrAny(cmi["id"]) 
                    author := ""; authorKey := ""; if a, ok := cmi["author"].(map[string]any); ok { author = toStrAny(a["displayName"]); authorKey = toStrAny(a["accountId"]); if authorKey == "" { authorKey = toStrAny(a["name"]) } }
                    body := sanitizeJiraText(toStrAny(cmi["body"]))
                    if created != nil { comments = append(comments, simpleComment{ID: id, Author: author+fmt.Sprintf(" (%s)", authorKey), Created: created.Format(time.RFC3339), Body: body}) } else { comments = append(comments, simpleComment{ID: id, Author: author+fmt.Sprintf(" (%s)", authorKey), Created: "", Body: body}) }
                }
                total, _ := cm["total"].(float64)
                startAtResp, _ := cm["startAt"].(float64)
                maxResp, _ := cm["maxResults"].(float64)
                if total == 0 { break }
                next := int(startAtResp) + int(maxResp)
                if float64(next) >= total { break }
                cStart = next
            }

            // changelog: use issue?expand=changelog (v2), and if total>fetched, page the rest via /changelog
            changes := make([]simpleChange, 0, 64)
            var haveHist int
            var totalHist int
            var startHist int
            if ii, err := s.jira.Issue(ctx, key, true); err == nil {
                if im2, ok := ii.(map[string]any); ok {
                    if ch, ok := im2["changelog"].(map[string]any); ok {
                        if t, ok := ch["total"].(float64); ok { totalHist = int(t) }
                        if sAt, ok := ch["startAt"].(float64); ok { startHist = int(sAt) }
                        if hs, ok := ch["histories"].([]any); ok {
                            for _, h0 := range hs {
                                hv, _ := h0.(map[string]any)
                                if hv == nil { continue }
                                at := parseTimeUTC(hv["created"]) 
                                items, _ := hv["items"].([]any)
                                for _, it0 := range items {
                                    itm, _ := it0.(map[string]any)
                                    if itm == nil { continue }
                                    field := toStrAny(itm["field"]) 
                                    from := toStrAny(itm["fromString"]) 
                                    to := toStrAny(itm["toString"]) 
                                    ts := ""; if at != nil { ts = at.Format(time.RFC3339) }
                                    changes = append(changes, simpleChange{At: ts, Field: field, From: from, To: to})
                                }
                                haveHist++
                            }
                        }
                    }
                }
            }
            // If server limited expand=changelog or did not report total, fetch remaining pages via /changelog
            if totalHist == 0 || totalHist > haveHist {
                hStart := startHist + haveHist
                for {
                    hresAny, _ := s.jira.Changelog(ctx, key, hStart, 100)
                    hm, _ := hresAny.(map[string]any)
                    var hvals []any
                    if vv, ok := hm["values"].([]any); ok { hvals = vv } else if vv, ok := hm["histories"].([]any); ok { hvals = vv }
                    if len(hvals) == 0 { break }
                    for _, h0 := range hvals {
                        hv, _ := h0.(map[string]any)
                        if hv == nil { continue }
                        at := parseTimeUTC(hv["created"]) 
                        items, _ := hv["items"].([]any)
                        for _, it0 := range items {
                            itm, _ := it0.(map[string]any)
                            if itm == nil { continue }
                            field := toStrAny(itm["field"]) 
                            from := toStrAny(itm["fromString"]) 
                            to := toStrAny(itm["toString"]) 
                            ts := ""; if at != nil { ts = at.Format(time.RFC3339) }
                            changes = append(changes, simpleChange{At: ts, Field: field, From: from, To: to})
                        }
                    }
                    // advance by response metadata if present, else by page length
                    if totalF, ok := hm["total"].(float64); ok { totalHist = int(totalF) }
                    if startF, ok := hm["startAt"].(float64); ok {
                        if maxF, ok2 := hm["maxResults"].(float64); ok2 {
                            next := int(startF) + int(maxF)
                            if next >= totalHist { break }
                            hStart = next
                            continue
                        }
                    }
                    hStart += len(hvals)
                    if hStart >= totalHist { break }
                }
            }

            // worklogs last 2d (robust pagination)
            var works []simpleWork
            wStart := 0
            for {
                wresAny, _ := s.jira.Worklogs(ctx, key, wStart, 100)
                wm, _ := wresAny.(map[string]any)
                warr, _ := wm["worklogs"].([]any)
                if len(warr) == 0 { break }
                for _, w0 := range warr {
                    wi, _ := w0.(map[string]any)
                    if wi == nil { continue }
                    started := parseTimeUTC(wi["started"]) 
                    if started == nil || started.Before(cutoff) { continue }
                    id := toStrAny(wi["id"]) 
                    author := ""; if a, ok := wi["author"].(map[string]any); ok { author = toStrAny(a["displayName"]) }
                    secs := 0; if v, ok := wi["timeSpentSeconds"].(float64); ok { secs = int(v) }
                    works = append(works, simpleWork{ID: id, Author: author, Started: started.Format(time.RFC3339), Seconds: secs})
                }
                total, _ := wm["total"].(float64)
                startAtResp, _ := wm["startAt"].(float64)
                maxResp, _ := wm["maxResults"].(float64)
                if total == 0 { break }
                next := int(startAtResp) + int(maxResp)
                if float64(next) >= total { break }
                wStart = next
            }

            payload := map[string]any{
                "board": boardName,
                "project": projectKey,
                "cutoff": cutoff.Format(time.RFC3339),
                "issue": map[string]any{
                    "key": key,
                    "summary": summary,
                    "status": status,
                    "type": func() string { if it, ok := fields["issuetype"].(map[string]any); ok { return toStrAny(it["name"]) }; return "" }(),
                    "updated": updated,
                    "created": created,
                    "priority": priority,
                    "assignee": map[string]any{"name": assignee, "key": assigneeKey},
                    "reporter": map[string]any{"name": reporter, "key": reporterKey},
                    "due_date": dueDate,
                    "labels": labels,
                    "estimate": estimate,
                    "subteam": subteam,
                    "service": service,
                    "epic": epicKey,
                    "comments": comments,
                    "history": func() any { if len(changes) == 0 { return []any{} }; return changes }(),
                    "history_meta": map[string]any{"total": totalHist, "from_expand": haveHist},
                    "worklogs": works,
                },
            }
            s.log.Info().Str("key", key).Int("hist_count", len(changes)).Int("hist_total", totalHist).Int("hist_from_expand", haveHist).Int("comment_count", len(comments)).Int("work_count", len(works)).Msg("jira test: collected counts")
            b, _ := json.MarshalIndent(payload, "", "  ")
            messages = append(messages, string(b))
        }
        if len(arr) < 50 { break }
        startAt += 50
    }

    if len(messages) == 0 { messages = []string{"No issues found in last 4 days."} }
    // Send each issue payload as its own chunked message to avoid exceeding Telegram limits
    for _, m := range messages {
        for _, part := range chunkText(m, 3500) {
            for _, chat := range s.cfg.TelegramChatIDs {
                if err := s.tg.SendMessagePlain(ctx, chat, part); err != nil { s.log.Error().Err(err).Msg("telegram send failed") }
            }
        }
    }
    return nil
}

// ensureCustomFields discovers common custom fields keys by name (Server/DC): Story Points, Epic Link, Subteam, Service
func (s *Service) ensureCustomFields(ctx context.Context) error {
    if s.customFields == nil { s.customFields = map[string]string{} }
    // Only return early if we already have all we care about
    if s.customFields["Story Points"] != "" && s.customFields["Epic Link"] != "" && s.customFields["Subteam"] != "" && s.customFields["Service"] != "" {
        return nil
    }
    // Seed from user-provided map first, to avoid naming mismatches
    if len(s.cfg.JiraFieldMap) > 0 {
        if id, ok := s.cfg.JiraFieldMap["Story Points"]; ok { s.customFields["Story Points"] = id }
        if id, ok := s.cfg.JiraFieldMap["Epic Link"]; ok { s.customFields["Epic Link"] = id }
        if id, ok := s.cfg.JiraFieldMap["Subteam"]; ok { s.customFields["Subteam"] = id }
        if id, ok := s.cfg.JiraFieldMap["Service"]; ok { s.customFields["Service"] = id }
    }
    fields, err := s.jira.Fields(ctx)
    if err != nil { return err }
    push := func(name, key string){ if name == "" || key == "" { return }; s.customFields[name] = key }
    if arr, ok := fields.([]map[string]any); ok {
        for _, f := range arr { name, _ := f["name"].(string); key, _ := f["key"].(string); id, _ := f["id"].(string)
            ln := strings.ToLower(strings.TrimSpace(name))
            if ln == "story points" { push("Story Points", key); if s.customFields["Story Points"] == "" { push("Story Points", id) } }
            if ln == "epic link" { push("Epic Link", key); if s.customFields["Epic Link"] == "" { push("Epic Link", id) } }
            if ln == "subteam" || strings.Contains(ln, "sub team") { push("Subteam", key); if s.customFields["Subteam"] == "" { push("Subteam", id) } }
            if ln == "service" || strings.Contains(ln, "service") { push("Service", key); if s.customFields["Service"] == "" { push("Service", id) } }
        }
        }
    if arrAny, ok := fields.([]any); ok {
        for _, f0 := range arrAny { if f, _ := f0.(map[string]any); f != nil {
            name, _ := f["name"].(string); key, _ := f["key"].(string); id, _ := f["id"].(string)
            ln := strings.ToLower(strings.TrimSpace(name))
            if ln == "story points" { push("Story Points", key); if s.customFields["Story Points"] == "" { push("Story Points", id) } }
            if ln == "epic link" { push("Epic Link", key); if s.customFields["Epic Link"] == "" { push("Epic Link", id) } }
            if ln == "subteam" || strings.Contains(ln, "sub team") { push("Subteam", key); if s.customFields["Subteam"] == "" { push("Subteam", id) } }
            if ln == "service" || strings.Contains(ln, "service") { push("Service", key); if s.customFields["Service"] == "" { push("Service", id) } }
        }}
    }
    if len(s.customFields) > 0 { s.log.Info().Fields(map[string]any{"custom_fields": s.customFields}).Msg("jira custom fields discovered") }
    return nil
}

func (s *Service) GetLastRun(ctx context.Context) (any, error) {
    return s.repo.GetLastRun(ctx)
}

func (s *Service) ensureBoards(ctx context.Context) error {
    if len(s.boardIDs) > 0 || len(s.cfg.JiraBoardNames) == 0 { return nil }
    ids, err := s.jira.ResolveBoardsByNames(ctx, s.cfg.JiraBoardNames)
    if err != nil { return err }
    s.boardIDs = ids
    // seed status map defaults for each board
    for _, id := range ids { _ = s.repo.SeedStatusMap(ctx, id) }
    return nil
}

// ensureFlaggedFields discovers Jira custom field ids that correspond to Flagged
func (s *Service) ensureFlaggedFields(ctx context.Context) error {
    if len(s.flaggedFields) > 0 { return nil }
    fields, err := s.jira.Fields(ctx)
    if err != nil { return err }
    var out []string
    // Always include the literal name for safety
    out = append(out, "Flagged")
    if arr, ok := fields.([]map[string]any); ok {
        for _, f := range arr {
            name, _ := f["name"].(string)
            key, _ := f["key"].(string)
            id, _ := f["id"].(string)
            if strings.EqualFold(name, "Flagged") {
                if key != "" { out = append(out, key) }
                if id != "" { out = append(out, id) }
            }
        }
    } else if arrAny, ok := fields.([]any); ok {
        for _, f0 := range arrAny {
            if f, _ := f0.(map[string]any); f != nil {
                name, _ := f["name"].(string)
                key, _ := f["key"].(string)
                id, _ := f["id"].(string)
                if strings.EqualFold(name, "Flagged") {
                    if key != "" { out = append(out, key) }
                    if id != "" { out = append(out, id) }
                }
            }
        }
    }
    s.flaggedFields = out
    if len(out) > 0 { s.log.Info().Strs("flagged_fields", out).Msg("jira flagged fields discovered") }
    return nil
}

// ---- ETL helpers (skeleton v1) ----
func parseTimeUTC(v any) *time.Time {
    s, _ := v.(string)
    if s == "" { return nil }
    layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000-0700", "2006-01-02T15:04:05-0700"}
    for _, l := range layouts {
        if t, err := time.Parse(l, s); err == nil {
            tt := t.UTC(); return &tt
        }
    }
    return nil
}

func toStrAny(v any) string {
    if v == nil { return "" }
    if s, ok := v.(string); ok { return s }
    return fmt.Sprintf("%v", v)
}

// optionToString extracts Jira option value objects: map with keys like value/self/id
func optionToString(v any) string {
    if v == nil { return "" }
    switch t := v.(type) {
    case string:
        return t
    case map[string]any:
        if s, ok := t["value"].(string); ok { return s }
        if name, ok := t["name"].(string); ok { return name }
        return toStrAny(v)
    case []any:
        vals := make([]string, 0, len(t))
        for _, it := range t {
            switch m := it.(type) {
            case map[string]any:
                if s, ok := m["value"].(string); ok { vals = append(vals, s); continue }
                if name, ok := m["name"].(string); ok { vals = append(vals, name); continue }
            case string:
                vals = append(vals, m)
            }
        }
        return strings.Join(vals, ", ")
    default:
        return toStrAny(v)
    }
}

// sanitizeJiraText removes Jira markup like {code} ... {code}, {color}, and converts CRLF
func sanitizeJiraText(s string) string {
    if s == "" { return s }
    // Strip {code[:lang]} blocks markers
    replacers := []struct{ old, new string }{
        {"\r\n", "\n"}, {"\r", "\n"},
        {"{code}", ""}, {"{noformat}", ""}, {"{panel}", ""},
        {"{color:#ff0000}", ""}, {"{color}", ""},
    }
    out := s
    for _, r := range replacers { out = strings.ReplaceAll(out, r.old, r.new) }
    // Remove any {code:lang} style openers
    if idx := strings.Index(out, "{code:"); idx != -1 {
        out = strings.ReplaceAll(out, "{code:java}", "")
        out = strings.ReplaceAll(out, "{code:json}", "")
        out = strings.ReplaceAll(out, "{code:sql}", "")
    }
    return out
}

// runETL paginates issues updated within the window and upserts core fields
func (s *Service) runETL(ctx context.Context, sinceDays int) (int, error) {
    issuesScanned := 0
    baseJQL := s.cfg.JiraDefaultJQL
    if baseJQL == "" { baseJQL = fmt.Sprintf("updated >= -%dd", sinceDays) }

    processJQL := func(jql string) (int, error) {
        count := 0
        fetch := func(startAt, max int) (map[string]any, error) {
            if len(s.boardIDs) > 0 {
                res, err := s.jira.BoardIssues(ctx, s.boardIDs[0], startAt, max, jql)
                if err != nil { return nil, err }
                m, _ := res.(map[string]any)
                if m == nil { return map[string]any{}, nil }
                return m, nil
            }
            res, err := s.jira.Search(ctx, jql, startAt, max)
            if err != nil { return nil, err }
            m, _ := res.(map[string]any)
            if m == nil { return map[string]any{}, nil }
            return m, nil
        }
        startAt := 0
        for {
            pageAny, err := fetch(startAt, 50)
            if err != nil { return count, err }
            arr, _ := pageAny["issues"].([]any)
            if len(arr) == 0 { break }
            // bounded worker pool per page
            workerCount := s.cfg.WorkersJira
            if workerCount <= 0 { workerCount = 6 }
            jobs := make(chan map[string]any)
            var wg sync.WaitGroup
            for w := 0; w < workerCount; w++ {
                wg.Add(1)
                go func(){
                    defer wg.Done()
                    for im := range jobs {
                        s.etlProcessIssue(ctx, im)
                    }
                }()
            }
            for _, it := range arr { if im, _ := it.(map[string]any); im != nil { jobs <- im; count++ } }
            close(jobs)
            wg.Wait()
            if len(arr) < 50 { break }
            startAt += 50
        }
        return count, nil
    }

    // Process base window
    n1, err := processJQL(baseJQL)
    issuesScanned += n1
    if err != nil { return issuesScanned, err }
    // Optionally process expedite JQL
    if strings.TrimSpace(s.cfg.JiraExpediteJQL) != "" {
        n2, err := processJQL(s.cfg.JiraExpediteJQL)
        issuesScanned += n2
        if err != nil { return issuesScanned, err }
    }
    return issuesScanned, nil
}

func derefTime(t *time.Time) time.Time { if t == nil { return time.Time{} }; return *t }

// chunkText splits text into chunks of up to max runes, attempting to break on line boundaries.
func chunkText(s string, max int) []string {
    if max <= 0 { return []string{s} }
    var chunks []string
    lines := strings.Split(s, "\n")
    cur := ""
    curlen := 0
    for _, ln := range lines {
        rl := len([]rune(ln))
        // If a single line exceeds max, hard-split the line
        if rl > max {
            // flush current first
            if curlen > 0 { chunks = append(chunks, cur); cur = ""; curlen = 0 }
            r := []rune(ln)
            for i := 0; i < rl; i += max {
                j := i + max
                if j > rl { j = rl }
                chunks = append(chunks, string(r[i:j]))
            }
            continue
        }
        // Add to current chunk if fits
        // account for newline when appending to non-empty cur
        extra := rl
        if curlen > 0 { extra += 1 }
        if curlen+extra > max {
            chunks = append(chunks, cur)
            cur = ln
            curlen = rl
        } else {
            if curlen == 0 { cur = ln; curlen = rl } else { cur += "\n" + ln; curlen += extra }
        }
    }
    if curlen > 0 { chunks = append(chunks, cur) }
    if len(chunks) == 0 { chunks = []string{""} }
    return chunks
}

func weekStart(t time.Time) time.Time {
    weekday := int(t.Weekday())
    if weekday == 0 { weekday = 7 }
    delta := time.Duration(weekday-1) * 24 * time.Hour
    day := t.Add(-delta)
    return time.Date(day.Year(), day.Month(), day.Day(), 0,0,0,0, day.Location())
}


// renderDigest builds a simple MarkdownV2 summary
func (s *Service) renderDigest(kpis map[string]float64, findings []map[string]any) string {
    esc := func(in string) string {
        repl := []string{"_","\\_","*","\\*","[","\\[","]","\\]","(","\\(",")","\\)","~","\\~","`","\\`",">","\\>","#","\\#","+","\\+","-","\\-","=","\\=","|","\\|","{","\\{","}","\\}",".","\\.","!","\\!"}
        for i := 0; i < len(repl); i+=2 { in = strings.ReplaceAll(in, repl[i], repl[i+1]) }
        return in
    }
    b := &strings.Builder{}
    fmt.Fprintf(b, "*Agile Pulse*\n")
    fmt.Fprintf(b, "Weekly summary\n\n")
    fmt.Fprintf(b, "*Throughput:* %d\n", int(kpis["throughput_total"]))
    fmt.Fprintf(b, "*WIP:* %d\n", int(kpis["wip_count"]))
    fmt.Fprintf(b, "*Lead avg:* %.1fd\n", kpis["lead_time_days_avg"]) 
    fmt.Fprintf(b, "*Cycle avg:* %.1fd\n\n", kpis["cycle_time_days_avg"]) 
    if len(findings) > 0 {
        fmt.Fprintf(b, "*Findings:*\n")
        limit := findings
        if len(limit) > 8 { limit = limit[:8] }
        for i, f := range limit {
            js, _ := json.Marshal(f)
            fmt.Fprintf(b, "%d\\. %s\n", i+1, esc(string(js)))
        }
    }
    return b.String()
}

func (s *Service) etlProcessIssue(ctx context.Context, im map[string]any) {
	fields, _ := im["fields"].(map[string]any)
	key := toStrAny(im["key"])
	if key == "" { return }
	proj := ""; if pj, ok := fields["project"].(map[string]any); ok { proj = toStrAny(pj["key"]) }
	typ := ""; if tp, ok := fields["issuetype"].(map[string]any); ok { typ = toStrAny(tp["name"]) }
	pri := ""; if pr, ok := fields["priority"].(map[string]any); ok { pri = toStrAny(pr["name"]) }
	asg := ""; if as, ok := fields["assignee"].(map[string]any); ok { asg = toStrAny(as["displayName"]) }
	rep := ""; if rp, ok := fields["reporter"].(map[string]any); ok { rep = toStrAny(rp["displayName"]) }
	st := ""; if ss, ok := fields["status"].(map[string]any); ok { st = toStrAny(ss["name"]) }
	created := parseTimeUTC(fields["created"])
	updated := parseTimeUTC(fields["updated"])
	done := parseTimeUTC(fields["resolutiondate"])
	due := parseTimeUTC(fields["duedate"])
	var points *float64
	if v, ok := fields["customfield_10016"].(float64); ok { tmp := v; points = &tmp }
	var labels []string
	if lv, ok := fields["labels"].([]any); ok { for _, x := range lv { if s, ok := x.(string); ok { labels = append(labels, s) } } }
	subteam := toStrAny(fields["subteam"]) 
	service := toStrAny(fields["service"]) 
	epicKey := ""; if ep, ok := fields["epic"].(map[string]any); ok { epicKey = toStrAny(ep["key"]) }

	issueID, _ := s.repo.UpsertIssue(ctx, domain.Issue{
		Key:           key,
		Project:       proj,
		Type:          typ,
		Priority:      pri,
		Assignee:      asg,
		Reporter:      rep,
		StatusLast:    st,
		CreatedAtJira: created,
		UpdatedAtJira: updated,
		DoneAt:        done,
		DueAt:         due,
		PointsEstimate: points,
		Labels:        labels,
		Subteam:       subteam,
		Service:       service,
		EpicKey:       epicKey,
	})
	// fetch comments
	var comments []domain.Comment
	cStart := 0
	for {
		cresAny, _ := s.jira.Comments(ctx, key, cStart, 100)
		cm, _ := cresAny.(map[string]any)
		carr, _ := cm["comments"].([]any)
		if len(carr) == 0 { break }
		for _, c0 := range carr {
			cmi, _ := c0.(map[string]any)
			if cmi == nil { continue }
			extID := toStrAny(cmi["id"])
			author := ""; if a, ok := cmi["author"].(map[string]any); ok { author = toStrAny(a["displayName"]) }
			at := parseTimeUTC(cmi["created"]) 
			body := toStrAny(cmi["body"])
			comments = append(comments, domain.Comment{IssueID: issueID, ExtID: extID, Author: author, Body: body, At: derefTime(at)})
		}
		if len(carr) < 100 { break }
		cStart += 100
	}
	if len(comments) > 0 { _ = s.repo.BulkInsertComments(ctx, comments) }
	// changelog
	var events []domain.Event
	if ii, err := s.jira.Issue(ctx, key, true); err == nil {
		if im2, ok := ii.(map[string]any); ok {
			if ch, ok := im2["changelog"].(map[string]any); ok {
				if hs, ok := ch["histories"].([]any); ok {
					for _, h0 := range hs {
						hv, _ := h0.(map[string]any)
						if hv == nil { continue }
						at := parseTimeUTC(hv["created"]) 
						items, _ := hv["items"].([]any)
						for _, it0 := range items {
							itm, _ := it0.(map[string]any)
							if itm == nil { continue }
							fieldName := toStrAny(itm["field"])
							fieldID := toStrAny(itm["fieldId"]) 
							field := fieldName
							if field == "" { field = fieldID }
							if fieldName == "Flagged" || fieldID == "Flagged" { field = "Flagged" }
							from := toStrAny(itm["fromString"])
							to := toStrAny(itm["toString"])
							events = append(events, domain.Event{IssueID: issueID, Kind: "changelog", Field: field, FromVal: from, ToVal: to, At: derefTime(at)})
						}
					}
				}
			}
		}
	}
	if len(events) > 0 { _ = s.repo.BulkInsertEvents(ctx, events) }
	// worklogs
	var wls []domain.Worklog
	wStart := 0
	for {
		wresAny, _ := s.jira.Worklogs(ctx, key, wStart, 100)
		wm, _ := wresAny.(map[string]any)
		warr, _ := wm["worklogs"].([]any)
		if len(warr) == 0 { break }
		for _, w0 := range warr {
			wi, _ := w0.(map[string]any)
			if wi == nil { continue }
			extID := toStrAny(wi["id"])
			author := ""; if a, ok := wi["author"].(map[string]any); ok { author = toStrAny(a["displayName"]) }
			started := parseTimeUTC(wi["started"]) 
			secs := 0; if v, ok := wi["timeSpentSeconds"].(float64); ok { secs = int(v) }
			wls = append(wls, domain.Worklog{IssueID: issueID, ExtID: extID, Author: author, StartedAt: derefTime(started), Seconds: secs})
		}
		if len(warr) < 100 { break }
		wStart += 100
	}
	if len(wls) > 0 { _ = s.repo.BulkInsertWorklogs(ctx, wls) }
}

