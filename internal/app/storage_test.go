package app

import (
	"os"
	"testing"
	"time"
)

// TestStorageInsertEvent ensures OpenAndInit can be called multiple times
// (simulating separate processes) without overwriting existing data.
func TestStorageInsertEvent(t *testing.T) {
	path := "./test_events.db"
	_ = os.Remove(path)

	// First open+init and insert one event
	st1, err := OpenAndInit(path)
	if err != nil {
		t.Fatalf("OpenAndInit (1): %v", err)
	}
	ev1 := Event{
		EventTime:    time.Now(),
		RawEventType: "删除",
		EventType:    "DELETE",
		DirPath:      `\\host\dir`,
		FilePath:     `\\host\dir\\1.jpg`,
		OldFilePath:  "",
	}
	id1, err := st1.InsertEvent(&ev1)
	if err != nil {
		st1.Close()
		t.Fatalf("InsertEvent (1): %v", err)
	}
	if id1 == 0 {
		st1.Close()
		t.Fatalf("expected non-zero id1")
	}

	// close first handle to simulate process exit
	if err := st1.Close(); err != nil {
		t.Fatalf("Close (1): %v", err)
	}

	// Re-open (OpenAndInit) and insert a second event
	st2, err := OpenAndInit(path)
	if err != nil {
		t.Fatalf("OpenAndInit (2): %v", err)
	}
	defer func() {
		st2.Close()
		os.Remove(path)
	}()

	ev2 := Event{
		EventTime:    time.Now().Add(time.Second),
		RawEventType: "新增",
		EventType:    "CREATE",
		DirPath:      `\\host\dir`,
		FilePath:     `\\host\dir\\2.jpg`,
		OldFilePath:  "",
	}
	id2, err := st2.InsertEvent(&ev2)
	if err != nil {
		t.Fatalf("InsertEvent (2): %v", err)
	}
	if id2 == 0 {
		t.Fatalf("expected non-zero id2")
	}
	if id2 == id1 {
		t.Fatalf("expected different ids: id1=%d id2=%d", id1, id2)
	}

	// read back both via Storage GetEventByID
	got1, err := st2.GetEventByID(id1)
	if err != nil {
		t.Fatalf("GetEventByID id1: %v", err)
	}
	if got1.EventType != ev1.EventType {
		t.Fatalf("event_type mismatch (1): got %q want %q", got1.EventType, ev1.EventType)
	}

	got2, err := st2.GetEventByID(id2)
	if err != nil {
		t.Fatalf("GetEventByID id2: %v", err)
	}
	if got2.EventType != ev2.EventType {
		t.Fatalf("event_type mismatch (2): got %q want %q", got2.EventType, ev2.EventType)
	}

	// allow small delta on time for both
	delta1 := got1.EventTime.Sub(ev1.EventTime.UTC())
	if delta1 < 0 {
		delta1 = -delta1
	}
	if delta1 > 5*time.Second {
		t.Fatalf("event_time mismatch (1): got %v want %v (delta %v)", got1.EventTime, ev1.EventTime.UTC(), delta1)
	}
	delta2 := got2.EventTime.Sub(ev2.EventTime.UTC())
	if delta2 < 0 {
		delta2 = -delta2
	}
	if delta2 > 5*time.Second {
		t.Fatalf("event_time mismatch (2): got %v want %v (delta %v)", got2.EventTime, ev2.EventTime.UTC(), delta2)
	}
}
