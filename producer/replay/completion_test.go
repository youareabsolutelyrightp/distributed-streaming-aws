package replay

import "testing"

func TestQueryCompleteIdentityIsQuerySpecific(t *testing.T) {
	first := stableQueryCompleteID("run", "query1")
	if len(first) != 64 {
		t.Fatalf("control ID has length %d", len(first))
	}
	if first == stableQueryCompleteID("run", "query2") {
		t.Fatal("two queries received the same completion identity")
	}
}
