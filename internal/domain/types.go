package domain

import "time"

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

type Event struct {
    ID       int64
    IssueID  int64
    Kind     string
    Field    string
    FromVal  string
    ToVal    string
    At       time.Time
}

type Comment struct {
    ID       int64
    IssueID  int64
    ExtID    string
    Author   string
    At       time.Time
    Body     string
}

type Worklog struct {
    ID         int64
    IssueID    int64
    ExtID      string
    Author     string
    StartedAt  time.Time
    Seconds    int
}

type Finding struct {
    ID        int64
    IssueID   int64
    WeekStart time.Time
    Data      map[string]any
}

