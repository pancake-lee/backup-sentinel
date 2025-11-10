package app

import (
	"os"
	"testing"
	"time"
)

// Test that DELETE and CREATE within 1s and same basename are merged
func TestDeleteCreateMatchWithin1s(t *testing.T) {
	path := "./test_match.db"
	_ = os.Remove(path)
	defer os.Remove(path)

	st, err := OpenAndInit(path)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer st.Close()

	baseTime := time.Now().Add(-10 * time.Second)
	del := Event{EventTime: baseTime, EventType: EventType_DELETE, FilePath: "dir/1.jpg"}
	did, err := st.InsertEvent(&del)
	if err != nil {
		t.Fatalf("insert delete: %v", err)
	}
	crt := Event{EventTime: baseTime.Add(500 * time.Millisecond), EventType: EventType_CREATE, FilePath: "dir/1.jpg"}
	cid, err := st.InsertEvent(&crt)
	if err != nil {
		t.Fatalf("insert create: %v", err)
	}

	res, err := GetAndFixedPendingEvents(st)
	if err != nil {
		t.Fatalf("get fixed pending: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 pending to process, got %d", len(res))
	}
	if res[0].ID != did {
		t.Fatalf("expected to process delete id %d as move, got %d", did, res[0].ID)
	}
	// ensure create was skipped
	row := st.db.QueryRow("SELECT processed FROM file_events WHERE id = ?", cid)
	var processed int
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan processed: %v", err)
	}
	if processed != 2 {
		t.Fatalf("expected create processed=2, got %d", processed)
	}
}

// Test that events separated by >1s are not merged
func TestDeleteCreateNotMatchBeyond1s(t *testing.T) {
	path := "./test_match2.db"
	_ = os.Remove(path)
	defer os.Remove(path)

	st, err := OpenAndInit(path)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer st.Close()

	baseTime := time.Now().Add(-10 * time.Second)
	del := Event{EventTime: baseTime, EventType: EventType_DELETE, FilePath: "dir/2.jpg"}
	did, err := st.InsertEvent(&del)
	if err != nil {
		t.Fatalf("insert delete: %v", err)
	}
	crt := Event{EventTime: baseTime.Add(1500 * time.Millisecond), EventType: EventType_CREATE, FilePath: "dir/2.jpg"}
	cid, err := st.InsertEvent(&crt)
	if err != nil {
		t.Fatalf("insert create: %v", err)
	}

	res, err := GetAndFixedPendingEvents(st)
	if err != nil {
		t.Fatalf("get fixed pending: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 pending to process, got %d", len(res))
	}
	// both ids present
	if res[0].ID != did || res[1].ID != cid {
		t.Fatalf("unexpected order or ids: %v", res)
	}
}
