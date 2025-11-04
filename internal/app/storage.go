package app

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/glebarez/sqlite"
)

// Storage wraps an sqlite DB connection.
type Storage struct {
	db *sql.DB
}

// Open opens (or creates) the sqlite database at path. It enables WAL mode.
func Open(path string) (*Storage, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// use driver specific Exec to set pragmas
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set journal_mode: %w", err)
	}

	return &Storage{db: db}, nil
}

// OpenAndInit opens the sqlite database at path and ensures schema is initialized.
// It is a convenience wrapper used by producer/consumer to avoid repeating InitSchema.
func OpenAndInit(path string) (*Storage, error) {
	s, err := Open(path)
	if err != nil {
		return nil, err
	}
	if err := s.InitSchema(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the DB connection.
func (s *Storage) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// InitSchema creates the file_events table if not exists.
func (s *Storage) InitSchema() error {
	const schema = `CREATE TABLE IF NOT EXISTS file_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		event_time DATETIME NOT NULL,
		event_type TEXT NOT NULL,
		raw_event_type TEXT,
		dir_path TEXT NOT NULL,
		file_path TEXT NOT NULL,
		old_file_path TEXT,
		processed INTEGER DEFAULT 0
	);`

	_, err := s.db.Exec(schema)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	return nil
}

// InsertEvent inserts the Event and returns the inserted row id.
func (s *Storage) InsertEvent(e Event) (int64, error) {
	const query = `INSERT INTO file_events (event_time, event_type, raw_event_type, dir_path, file_path, old_file_path) VALUES (?, ?, ?, ?, ?, ?)`

	// Use time in UTC for storage
	res, err := s.db.Exec(query, e.EventTime.UTC().Format(time.RFC3339), e.EventType, e.RawEventType, e.DirPath, e.FilePath, e.OldFilePath)
	if err != nil {
		return 0, fmt.Errorf("insert event: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("last insert id: %w", err)
	}
	return id, nil
}

// GetEventByID returns the Event stored with the given id.
func (s *Storage) GetEventByID(id int64) (Event, error) {
	const query = `SELECT event_time, event_type, raw_event_type, dir_path, file_path, old_file_path FROM file_events WHERE id = ?`

	var (
		eventTimeStr string
		eventType    string
		rawEventType sql.NullString
		dirPath      string
		filePath     string
		oldFilePath  sql.NullString
	)

	row := s.db.QueryRow(query, id)
	if err := row.Scan(&eventTimeStr, &eventType, &rawEventType, &dirPath, &filePath, &oldFilePath); err != nil {
		return Event{}, fmt.Errorf("scan event: %w", err)
	}

	t, err := time.Parse(time.RFC3339, eventTimeStr)
	if err != nil {
		return Event{}, fmt.Errorf("parse time: %w", err)
	}

	ev := Event{
		EventTime:    t,
		RawEventType: "",
		EventType:    eventType,
		DirPath:      dirPath,
		FilePath:     filePath,
		OldFilePath:  "",
	}
	if rawEventType.Valid {
		ev.RawEventType = rawEventType.String
	}
	if oldFilePath.Valid {
		ev.OldFilePath = oldFilePath.String
	}

	return ev, nil
}

// PendingEvent is an event read from storage including its DB id.
type PendingEvent struct {
	ID int64
	Event
}

// GetPendingEvents returns up to `limit` events that are not yet processed,
// ordered by event_time ascending.
func (s *Storage) GetPendingEvents(limit int) ([]PendingEvent, error) {
	const query = `SELECT id, event_time, event_type, raw_event_type, dir_path, file_path, old_file_path FROM file_events WHERE processed = 0 ORDER BY event_time ASC LIMIT ?`

	rows, err := s.db.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending: %w", err)
	}
	defer rows.Close()

	var res []PendingEvent
	for rows.Next() {
		var id int64
		var (
			eventTimeStr string
			eventType    string
			rawEventType sql.NullString
			dirPath      string
			filePath     string
			oldFilePath  sql.NullString
		)
		if err := rows.Scan(&id, &eventTimeStr, &eventType, &rawEventType, &dirPath, &filePath, &oldFilePath); err != nil {
			return nil, fmt.Errorf("scan pending row: %w", err)
		}
		t, err := time.Parse(time.RFC3339, eventTimeStr)
		if err != nil {
			return nil, fmt.Errorf("parse time: %w", err)
		}
		ev := Event{
			EventTime:    t,
			RawEventType: "",
			EventType:    eventType,
			DirPath:      dirPath,
			FilePath:     filePath,
			OldFilePath:  "",
		}
		if rawEventType.Valid {
			ev.RawEventType = rawEventType.String
		}
		if oldFilePath.Valid {
			ev.OldFilePath = oldFilePath.String
		}

		res = append(res, PendingEvent{ID: id, Event: ev})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}
	return res, nil
}

// MarkProcessed marks the event with given id as processed (1).
func (s *Storage) MarkProcessed(id int64) error {
	const query = `UPDATE file_events SET processed = 1 WHERE id = ?`
	res, err := s.db.Exec(query, id)
	if err != nil {
		return fmt.Errorf("mark processed exec: %w", err)
	}
	if ra, err := res.RowsAffected(); err == nil {
		if ra == 0 {
			return fmt.Errorf("mark processed: no rows affected for id %d", id)
		}
	}
	return nil
}
