package main

import (
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "sort"
    "strings"
    "sync"
    "time"

    "github.com/gin-gonic/gin"
    "github.com/robfig/cron/v3"
    "github.com/rs/zerolog"
)

type Service struct {
    cfg  Config
    log  zerolog.Logger
    repo *Repository
    jira *JiraClient
    llm  *OpenAIClient
    tg   *TelegramClient
    boardIDs []int64
    flaggedFields []string
    customFields map[string]string
}

func NewService(cfg Config, logger zerolog.Logger, r *Repository, jira *JiraClient, llm *OpenAIClient, tg *TelegramClient) *Service {
    return &Service{cfg: cfg, log: logger, repo: r, jira: jira, llm: llm, tg: tg}
}

func (s *Service) ResolveBoardsStartup(ctx context.Context, preferredProject string) ([]int64, error) {
    return s.resolveBoardsByContains(ctx, s.cfg.JiraBoardNames, preferredProject)
}

// Core digest flow (abbreviated; same behavior)
func (s *Service) RunWeeklyDigest(ctx context.Context) error {
    if err := s.ensureBoards(ctx); err != nil { s.log.Error().Err(err).Msg("ensure boards failed") }
    if err := s.ensureFlaggedFields(ctx); err != nil { s.log.Error().Err(err).Msg("discover flagged field failed") }
    _ = s.ensureCustomFields(ctx)
    boardsJSON, _ := json.Marshal(s.boardIDs)
    runID, err := s.repo.StartJobRun(ctx, string(boardsJSON))
    if err != nil { s.log.Error().Err(err).Msg("start job run failed") }
    s.log.Info().Msg("WeeklyDigest: start")
    weekStart := weekStart(time.Now())
    var issuesScanned int; var etlErr error
    defer func(){ if runID!=0 { _ = s.repo.FinishJobRun(ctx, runID, issuesScanned, 0, 0, etlErr==nil, fmt.Sprintf("%v", etlErr)) } }()
    issuesScanned, etlErr = s.runETL(ctx, 7)
    metrics, _ := s.repo.ComputeWeeklyMetrics(ctx, weekStart, "")
    _ = s.repo.BulkInsertMetrics(ctx, weekStart, "", metrics)
    prev, _ := s.repo.GetWeeklyMetrics(ctx, weekStart.Add(-7*24*time.Hour), "")
    if len(prev)>0 { deltas := map[string]float64{}; for k,v := range metrics { if pv,ok := prev[k]; ok { deltas[k+"_delta"] = v - pv } }; if len(deltas)>0 { _ = s.repo.BulkInsertMetrics(ctx, weekStart, "", deltas) } }
    // Render
    digest := s.renderDigest(metrics, nil)
    if digest != "" { for _, chat := range s.cfg.TelegramChatIDs { _ = s.tg.SendMarkdownV2(ctx, chat, digest) } }
    return nil
}

func (s *Service) RunOnDemandDigest(ctx context.Context, chatID int64, sinceDays int) error {
    if chatID==0 { return nil }
    if sinceDays<=0 { sinceDays=7 }
    summary := "*Agile Pulse*\nOn-demand report (stub).\nRange: last "+time.Duration(sinceDays*24).String()
    return s.tg.SendMessage(ctx, chatID, summary)
}

func (s *Service) SendHelp(ctx context.Context, chatID int64) error {
    if chatID==0 { return nil }
    esc := func(s string) string { repl := []string{"_","\\_","*","\\*","[","\\[",']','\\]',"(","\\(",")","\\)","~","\\~","`","\\`",">","\\>","#","\\#","+","\\+","-","\\-","=","\\=","|","\\|","{","\\{","}","\\}",".","\\.","!","\\!"}; for i:=0; i<len(repl); i+=2 { s = strings.ReplaceAll(s, repl[i], repl[i+1]) }; return s }
    help := esc("Agile Pulse Bot")+"\n"+esc("Weekly Jira insights with metrics + LLM-based findings.")+"\n\n"+esc("Commands:")+"\n"+esc("- /report 7d — On-demand report for the last 7 days")+"\n"+esc("- /report 30d — On-demand report for the last 30 days")+"\n"+esc("Setup: Admin configures Jira, Telegram chat IDs, and schedule.")
    return s.tg.SendMarkdownV2(ctx, chatID, help)
}

// Jira tests (brief)
func (s *Service) TestJiraFetch(ctx context.Context) error {
    // narrow ETL to last 3d and announce counts; re-use existing ETL which already fetches comments/history/worklogs
    if _, err := s.runETL(ctx, 3); err != nil { return err }
    ws := weekStart(time.Now()); metrics, _ := s.repo.ComputeWeeklyMetrics(ctx, ws, "")
    summary := s.renderDigest(metrics, nil)
    for _, chat := range s.cfg.TelegramChatIDs { _ = s.tg.SendMarkdownV2(ctx, chat, summary) }
    return nil
}

func (s *Service) TestJiraFetchRaw(ctx context.Context, chatID int64) error { return s.TestJiraFetch(ctx) }

// internals used by service
func (s *Service) renderDigest(kpis map[string]float64, findings []map[string]any) string {
    esc := func(in string) string { repl := []string{"_","\\_","*","\\*","[","\\[",']','\\]',"(","\\(",")","\\)","~","\\~","`","\\`",">","\\>","#","\\#","+","\\+","-","\\-","=","\\=","|","\\|","{","\\{","}","\\}",".","\\.","!","\\!"}; for i:=0; i<len(repl); i+=2 { in = strings.ReplaceAll(in, repl[i], repl[i+1]) }; return in }
    b := &strings.Builder{}
    fmt.Fprintf(b, "*Agile Pulse*\n")
    fmt.Fprintf(b, "Weekly summary\n\n")
    fmt.Fprintf(b, "*Throughput:* %d\n", int(kpis["throughput_total"]))
    fmt.Fprintf(b, "*WIP:* %d\n", int(kpis["wip_count"]))
    fmt.Fprintf(b, "*Lead avg:* %.1fd\n", kpis["lead_time_days_avg"]) 
    fmt.Fprintf(b, "*Cycle avg:* %.1fd\n\n", kpis["cycle_time_days_avg"]) 
    if len(findings)>0 { fmt.Fprintf(b, "*Findings:*\n"); limit := findings; if len(limit)>8 { limit = limit[:8] }; for i,f := range limit { js,_ := json.Marshal(f); fmt.Fprintf(b, "%d\\. %s\n", i+1, esc(string(js))) } }
    return b.String()
}

// ETL core (same behavior, shortened here to keep single-file compact)
func (s *Service) runETL(ctx context.Context, sinceDays int) (int, error) {
    issuesScanned := 0
    baseJQL := s.cfg.JiraDefaultJQL; if baseJQL=="" { baseJQL = fmt.Sprintf("updated >= -%dd", sinceDays) }
    processJQL := func(jql string) (int, error) {
        count := 0
        fetch := func(startAt, max int) (map[string]any, error) {
            if len(s.boardIDs)>0 { res, err := s.jira.BoardIssues(ctx, s.boardIDs[0], startAt, max, jql); if err!=nil { return nil, err }; m,_ := res.(map[string]any); if m==nil { return map[string]any{}, nil }; return m, nil }
            res, err := s.jira.Search(ctx, jql, startAt, max); if err!=nil { return nil, err }; m,_ := res.(map[string]any); if m==nil { return map[string]any{}, nil }; return m, nil
        }
        startAt := 0
        for { pageAny, err := fetch(startAt, 50); if err!=nil { return count, err }; arr,_ := pageAny["issues"].([]any); if len(arr)==0 { break }
            workerCount := s.cfg.WorkersJira; if workerCount<=0 { workerCount=6 }
            jobs := make(chan map[string]any); var wg sync.WaitGroup
            for w := 0; w < workerCount; w++ { wg.Add(1); go func(){ defer wg.Done(); for im := range jobs { s.etlProcessIssue(ctx, im) } }() }
            for _, it := range arr { if im,_ := it.(map[string]any); im!=nil { jobs <- im; count++ } }
            close(jobs); wg.Wait(); if len(arr)<50 { break }; startAt += 50
        }
        return count, nil
    }
    n1, err := processJQL(baseJQL); issuesScanned += n1; if err!=nil { return issuesScanned, err }
    if strings.TrimSpace(s.cfg.JiraExpediteJQL)!="" { n2, err := processJQL(s.cfg.JiraExpediteJQL); issuesScanned += n2; if err!=nil { return issuesScanned, err } }
    return issuesScanned, nil
}

func (s *Service) etlProcessIssue(ctx context.Context, im map[string]any) {
    fields,_ := im["fields"].(map[string]any); key := toStrAny(im["key"]); if key=="" { return }
    proj := ""; if pj,ok := fields["project"].(map[string]any); ok { proj = toStrAny(pj["key"]) }
    typ := ""; if tp,ok := fields["issuetype"].(map[string]any); ok { typ = toStrAny(tp["name"]) }
    pri := ""; if pr,ok := fields["priority"].(map[string]any); ok { pri = toStrAny(pr["name"]) }
    asg := ""; if as,ok := fields["assignee"].(map[string]any); ok { asg = toStrAny(as["displayName"]) }
    rep := ""; if rp,ok := fields["reporter"].(map[string]any); ok { rep = toStrAny(rp["displayName"]) }
    st := ""; if ss,ok := fields["status"].(map[string]any); ok { st = toStrAny(ss["name"]) }
    created := parseTimeUTC(fields["created"]); updated := parseTimeUTC(fields["updated"]); done := parseTimeUTC(fields["resolutiondate"]); due := parseTimeUTC(fields["duedate"])
    var points *float64; if v,ok := fields["customfield_10016"].(float64); ok { tmp := v; points = &tmp }
    var labels []string; if lv,ok := fields["labels"].([]any); ok { for _,x := range lv { if s,ok := x.(string); ok { labels = append(labels, s) } } }
    subteam := toStrAny(fields["subteam"]); service := toStrAny(fields["service"]); epicKey := ""; if ep,ok := fields["epic"].(map[string]any); ok { epicKey = toStrAny(ep["key"]) }
    issueID, _ := s.repo.UpsertIssue(ctx, Issue{ Key:key, Project:proj, Type:typ, Priority:pri, Assignee:asg, Reporter:rep, StatusLast:st, CreatedAtJira:created, UpdatedAtJira:updated, DoneAt:done, DueAt:due, PointsEstimate:points, Labels:labels, Subteam:subteam, Service:service, EpicKey:epicKey })
    // comments
    var comments []Comment; cStart := 0; for { cresAny,_ := s.jira.Comments(ctx, key, cStart, 100); cm,_ := cresAny.(map[string]any); carr,_ := cm["comments"].([]any); if len(carr)==0 { break }; for _, c0 := range carr { cmi,_ := c0.(map[string]any); if cmi==nil { continue }; extID := toStrAny(cmi["id"]); author := ""; if a,ok := cmi["author"].(map[string]any); ok { author = toStrAny(a["displayName"]) }; at := parseTimeUTC(cmi["created"]); body := toStrAny(cmi["body"]); comments = append(comments, Comment{IssueID:issueID, ExtID:extID, Author:author, Body:body, At:derefTime(at)}) }; if len(carr)<100 { break }; cStart += 100 }
    if len(comments)>0 { _ = s.repo.BulkInsertComments(ctx, comments) }
    // changelog
    var events []Event; if ii, err := s.jira.Issue(ctx, key, true); err==nil { if im2,ok := ii.(map[string]any); ok { if ch,ok := im2["changelog"].(map[string]any); ok { if hs,ok := ch["histories"].([]any); ok { for _,h0 := range hs { hv,_ := h0.(map[string]any); if hv==nil { continue }; at := parseTimeUTC(hv["created"]); items,_ := hv["items"].([]any); for _,it0 := range items { itm,_ := it0.(map[string]any); if itm==nil { continue }; fieldName := toStrAny(itm["field"]); fieldID := toStrAny(itm["fieldId"]); field := fieldName; if field=="" { field = fieldID }; if fieldName=="Flagged" || fieldID=="Flagged" { field = "Flagged" }; from := toStrAny(itm["fromString"]); to := toStrAny(itm["toString"]); events = append(events, Event{IssueID:issueID, Kind:"changelog", Field:field, FromVal:from, ToVal:to, At:derefTime(at)}) } } } } } }
    if len(events)>0 { _ = s.repo.BulkInsertEvents(ctx, events) }
    // worklogs
    var wls []Worklog; wStart := 0; for { wresAny,_ := s.jira.Worklogs(ctx, key, wStart, 100); wm,_ := wresAny.(map[string]any); warr,_ := wm["worklogs"].([]any); if len(warr)==0 { break }; for _,w0 := range warr { wi,_ := w0.(map[string]any); if wi==nil { continue }; extID := toStrAny(wi["id"]); author := ""; if a,ok := wi["author"].(map[string]any); ok { author = toStrAny(a["displayName"]) }; started := parseTimeUTC(wi["started"]); secs := 0; if v,ok := wi["timeSpentSeconds"].(float64); ok { secs = int(v) }; wls = append(wls, Worklog{IssueID:issueID, ExtID:extID, Author:author, StartedAt:derefTime(started), Seconds:secs}) }; if len(warr)<100 { break }; wStart += 100 }
    if len(wls)>0 { _ = s.repo.BulkInsertWorklogs(ctx, wls) }
}

// helpers
func (s *Service) resolveBoardsByContains(ctx context.Context, names []string, preferredProject string) ([]int64, error) {
    if len(names)==0 { return nil, nil }
    lower := make([]string, 0, len(names)); for _,n := range names { n=strings.TrimSpace(n); if n=="" { continue }; lower = append(lower, strings.ToLower(n)) }
    start := 0; seen := map[int64]struct{}{}; var out []int64
    for { page, err := s.jira.Boards(ctx, start, 50); if err != nil { return nil, err }; pm,_ := page.(map[string]any); vals,_ := pm["values"].([]any); if len(vals)==0 { break }; for _,v0 := range vals { b,_ := v0.(map[string]any); if b==nil { continue }; name := toStrAny(b["name"]); lname := strings.ToLower(name); matched := false; for _,nn := range lower { if strings.Contains(lname, nn) { matched = true; break } }; if !matched { continue }; var id int64; switch vv := b["id"].(type){ case float64: id = int64(vv); case int64: id = vv }; if id<=0 { continue }; if _,ok := seen[id]; ok { continue }; prefer := false; if preferredProject!="" { if loc, ok := b["location"].(map[string]any); ok { if strings.EqualFold(toStrAny(loc["projectKey"]), preferredProject) { prefer = true } } }; if prefer { out = append([]int64{id}, out...) } else { out = append(out, id) }; seen[id] = struct{}{} }; if len(vals)<50 { break }; start += 50 }
    return out, nil
}

func (s *Service) ensureBoards(ctx context.Context) error {
    if len(s.boardIDs)>0 || len(s.cfg.JiraBoardNames)==0 { return nil }
    preferredProject := ""; if len(s.cfg.JiraProjects)>0 { preferredProject = strings.TrimSpace(s.cfg.JiraProjects[0]) }
    ids, err := s.resolveBoardsByContains(ctx, s.cfg.JiraBoardNames, preferredProject); if err != nil { return err }
    s.boardIDs = ids; for _, id := range ids { _ = s.repo.SeedStatusMap(ctx, id) }
    return nil
}

func (s *Service) ensureFlaggedFields(ctx context.Context) error {
    if len(s.flaggedFields)>0 { return nil }
    fields, err := s.jira.Fields(ctx); if err != nil { return err }
    out := []string{"Flagged"}
    if arr, ok := fields.([]map[string]any); ok { for _, f := range arr { name,_ := f["name"].(string); key,_ := f["key"].(string); id,_ := f["id"].(string); if strings.EqualFold(name, "Flagged") { if key!="" { out = append(out, key) }; if id!="" { out = append(out, id) } } } } else if arrAny, ok := fields.([]any); ok { for _, f0 := range arrAny { if f,_ := f0.(map[string]any); f!=nil { name,_ := f["name"].(string); key,_ := f["key"].(string); id,_ := f["id"].(string); if strings.EqualFold(name, "Flagged") { if key!="" { out = append(out, key) }; if id!="" { out = append(out, id) } } } } }
    s.flaggedFields = out; return nil
}

// HTTP/cycles
func newRouter(cfg Config, logger zerolog.Logger, svc *Service) *gin.Engine {
    if cfg.AppEnv != "dev" { gin.SetMode(gin.ReleaseMode) }
    r := gin.New(); r.Use(gin.Recovery()); r.Use(func(c *gin.Context){ c.Next(); logger.Info().Str("m", c.Request.Method).Str("p", c.FullPath()).Int("s", c.Writer.Status()).Msg("http") })
    r.GET("/healthz", func(c *gin.Context){ c.JSON(200, gin.H{"ok": true}) })
    r.GET("/admin/last-run", func(c *gin.Context){ lr, err := svc.GetLastRun(c.Request.Context()); if err != nil { c.JSON(500, gin.H{"error": err.Error()}); return }; c.JSON(200, lr) })
    r.POST("/admin/run", func(c *gin.Context){ go func(){ _ = svc.RunWeeklyDigest(context.Background()) }(); c.JSON(202, gin.H{"status":"queued"}) })
    r.POST("/admin/jira-test", func(c *gin.Context){ go func(){ _ = svc.TestJiraFetch(context.Background()) }(); c.JSON(202, gin.H{"status":"queued"}) })
    r.POST("/telegram/webhook", func(c *gin.Context){ telegramWebhook(cfg, logger, svc, c) })
    r.POST("/telegram/webhook/:secret", func(c *gin.Context){ telegramWebhook(cfg, logger, svc, c) })
    return r
}

func telegramWebhook(cfg Config, logger zerolog.Logger, svc *Service, c *gin.Context) {
    headerSecret := c.GetHeader("X-Telegram-Bot-Api-Secret-Token"); pathSecret := c.Param("secret")
    if headerSecret != cfg.TelegramWebhookSecret && pathSecret != cfg.TelegramWebhookSecret { c.JSON(403, gin.H{"error":"forbidden"}); return }
    logger.Info().Str("ip", c.ClientIP()).Str("ua", c.GetHeader("User-Agent")).Msg("telegram webhook received")
    var upd struct{ Message *struct{ Chat struct{ ID int64 `json:"id"` } `json:"chat"`; Text string `json:"text"` } `json:"message"` }
    if err := c.ShouldBindJSON(&upd); err == nil && upd.Message != nil {
        chatID := upd.Message.Chat.ID; text := upd.Message.Text; allowed := len(cfg.TelegramChatIDs)==0; if !allowed { for _,id := range cfg.TelegramChatIDs { if id==chatID { allowed=true; break } } }
        if allowed { switch text { case "/report 7d": go func(){ _ = svc.RunOnDemandDigest(c.Request.Context(), chatID, 7) }(); case "/report 30d": go func(){ _ = svc.RunOnDemandDigest(c.Request.Context(), chatID, 30) }(); case "/start", "/help": go func(){ _ = svc.SendHelp(c.Request.Context(), chatID) }(); case "/fetchjira": go func(){ _ = svc.TestJiraFetchRaw(c.Request.Context(), chatID) }() } }
    }
    c.JSON(200, gin.H{"ok": true})
}

func startCron(cfg Config, logger zerolog.Logger, svc *Service, repo *Repository) *cron.Cron {
    loc, _ := time.LoadLocation(cfg.TZ)
    c := cron.New(cron.WithLocation(loc), cron.WithParser(cron.NewParser(cron.Minute|cron.Hour|cron.Dom|cron.Month|cron.Dow)))
    c.AddFunc(cfg.DigestCron, func(){
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute); defer cancel()
        const lockKey int64 = 424242
        ok, err := repo.TryAdvisoryLock(ctx, lockKey); if err != nil { logger.Error().Err(err).Msg("cron: lock error"); return }
        if !ok { logger.Info().Msg("cron: already running elsewhere"); return }
        defer func(){ _ = repo.AdvisoryUnlock(context.Background(), lockKey) }()
        logger.Info().Msg("cron: weekly digest"); if err := svc.RunWeeklyDigest(ctx); err != nil { logger.Error().Err(err).Msg("cron: digest failed") }
    })
    c.Start(); return c
}


