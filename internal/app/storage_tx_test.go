package app

import (
	"os"
	"testing"
	"time"
)

func TestConvertDeleteToMoveAndSkipCreate(t *testing.T) {
	path := "./test_tx.db"
	_ = os.Remove(path)
	defer os.Remove(path)

	st, err := OpenAndInit(path)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer st.Close()

	// insert a DELETE event (old path)
	now := time.Now().Add(-2 * time.Second)
	del := Event{
		EventTime:    now,
		RawEventType: "删除",
		EventType:    EventType_DELETE,
		DirPath:      "d",
		FilePath:     "old.jpg",
	}
	delID, err := st.InsertEvent(&del)
	if err != nil {
		t.Fatalf("insert delete: %v", err)
	}

	// insert a CREATE event shortly after (within 1s)
	crt := Event{
		EventTime:    now.Add(500 * time.Millisecond),
		RawEventType: "新增",
		EventType:    EventType_CREATE,
		DirPath:      "d",
		FilePath:     "new.jpg",
	}
	crtID, err := st.InsertEvent(&crt)
	if err != nil {
		t.Fatalf("insert create: %v", err)
	}

	// perform transactional conversion
	if err := st.ConvertDeleteToMoveAndSkipCreate(delID, crtID, del.FilePath, crt.FilePath); err != nil {
		t.Fatalf("convert tx failed: %v", err)
	}

	// read back delete row
	got, err := st.GetEventByID(delID)
	if err != nil {
		t.Fatalf("get event del: %v", err)
	}
	if got.EventType != EventType_MOVE {
		t.Fatalf("expected delete converted to MOVE, got %s", got.EventType)
	}
	if got.FilePath != crt.FilePath {
		t.Fatalf("expected move file_path updated to %s, got %s", crt.FilePath, got.FilePath)
	}
	if got.OldFilePath != del.FilePath {
		t.Fatalf("expected old_file_path preserved as %s, got %s", del.FilePath, got.OldFilePath)
	}

	// check create row processed == 2
	row := st.db.QueryRow("SELECT processed FROM file_events WHERE id = ?", crtID)
	var processed int
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan processed: %v", err)
	}
	if processed != 2 {
		t.Fatalf("expected create processed=2, got %d", processed)
	}
}

func TestMarkSkipped(t *testing.T) {
	path := "./test_tx_skip.db"
	_ = os.Remove(path)
	defer os.Remove(path)

	st, err := OpenAndInit(path)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer st.Close()

	// insert a MODIFY event
	ev := Event{
		EventTime:    time.Now().Add(-2 * time.Second),
		RawEventType: "修改",
		EventType:    EventType_MODIFY,
		DirPath:      "d",
		FilePath:     "file.jpg",
	}
	id, err := st.InsertEvent(&ev)
	if err != nil {
		t.Fatalf("insert ev: %v", err)
	}

	if err := st.MarkSkipped(id); err != nil {
		t.Fatalf("MarkSkipped failed: %v", err)
	}

	row := st.db.QueryRow("SELECT processed FROM file_events WHERE id = ?", id)
	var processed int
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan processed: %v", err)
	}
	if processed != 2 {
		t.Fatalf("expected processed=2, got %d", processed)
	}
}
