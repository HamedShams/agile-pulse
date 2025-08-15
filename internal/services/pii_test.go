package services

import (
    "testing"
    "time"
    "github.com/HamedShams/agile-pulse/internal/domain"
)

func TestRedactPII_MasksCommonPatternsAndAliasesAuthors(t *testing.T) {
    comments := []domain.Comment{
        {Author: "Alice Smith", Body: "Reach me at alice@example.com or +1 555 123 4567. Token: secret=abcdEFGH1234", At: time.Now()},
        {Author: "Bob", Body: "Alice Smith shared https://example.com/path", At: time.Now()},
    }
    history := []domain.Event{
        {FromVal: "Contact alice@example.com", ToVal: "Visit https://example.com"},
    }
    payload := map[string]any{"comments": comments, "history": history}
    red := redactPII(payload)

    // Authors should be aliased consistently
    rc, ok := red["comments"].([]domain.Comment)
    if !ok || len(rc) != 2 { t.Fatalf("expected 2 comments, got %#v", red["comments"]) }
    if rc[0].Author == "Alice Smith" || rc[1].Author == "Alice Smith" {
        t.Fatalf("author name not aliased: %#v", rc)
    }
    if rc[0].Author == rc[1].Author {
        t.Fatalf("different authors should have different aliases: %#v", rc)
    }
    // Bodies scrubbed
    if rc[0].Body == comments[0].Body { t.Fatalf("expected body scrubbed") }
    if rc[1].Body == comments[1].Body { t.Fatalf("expected body scrubbed") }

    // History scrubbed
    rh, ok := red["history"].([]domain.Event)
    if !ok || len(rh) != 1 { t.Fatalf("expected 1 history, got %#v", red["history"]) }
    if rh[0].FromVal == history[0].FromVal || rh[0].ToVal == history[0].ToVal {
        t.Fatalf("expected history values scrubbed")
    }
}


