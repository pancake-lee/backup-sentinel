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
		cmd_file TEXT,
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
func (s *Storage) InsertEvent(e *Event) (int64, error) {
	const query = `INSERT INTO file_events (event_time, event_type, raw_event_type, dir_path, cmd_file, file_path, old_file_path) VALUES (?, ?, ?, ?, ?, ?, ?)`

	// Use time in UTC for storage
	res, err := s.db.Exec(query, e.EventTime.UTC().Format(time.RFC3339Nano), e.EventType, e.RawEventType, e.DirPath, e.CmdFile, e.FilePath, e.OldFilePath)
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
	const query = `SELECT event_time, event_type, raw_event_type, dir_path, cmd_file, file_path, old_file_path FROM file_events WHERE id = ?`

	var (
		eventTimeStr string
		eventType    string
		rawEventType sql.NullString
		dirPath      string
		cmdFile      sql.NullString
		filePath     string
		oldFilePath  sql.NullString
	)

	row := s.db.QueryRow(query, id)
	if err := row.Scan(&eventTimeStr, &eventType, &rawEventType, &dirPath, &cmdFile, &filePath, &oldFilePath); err != nil {
		return Event{}, fmt.Errorf("scan event: %w", err)
	}

	t, err := time.Parse(time.RFC3339Nano, eventTimeStr)
	if err != nil {
		return Event{}, fmt.Errorf("parse time: %w", err)
	}

	ev := Event{
		EventTime:    t,
		RawEventType: "",
		EventType:    EventType(eventType),
		DirPath:      dirPath,
		CmdFile:      "",
		FilePath:     filePath,
		OldFilePath:  "",
	}
	if rawEventType.Valid {
		ev.RawEventType = rawEventType.String
	}
	if cmdFile.Valid {
		ev.CmdFile = cmdFile.String
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

// GetPendingEvents returns the earliest unprocessed event (older than 2s)
// and any subsequent unprocessed events whose event_time is within 2 second
// after that earliest event. Results are ordered by event_time ascending.
func (s *Storage) GetPendingEvents() ([]PendingEvent, error) {
	// only consider events older than 2X to avoid racing with writer
	cutoff := time.Now().Add(-2 * rangeInterval).UTC().Format(time.RFC3339)

	// 1) find the earliest event_time among unprocessed events older than cutoff
	const minQuery = `SELECT MIN(event_time) FROM file_events WHERE processed = 0 AND event_time <= ?`
	var minEventTime sql.NullString
	if err := s.db.QueryRow(minQuery, cutoff).Scan(&minEventTime); err != nil {
		return nil, fmt.Errorf("query min event_time: %w", err)
	}
	if !minEventTime.Valid || minEventTime.String == "" {
		// no eligible events
		return nil, nil
	}

	tmin, err := time.Parse(time.RFC3339, minEventTime.String)
	if err != nil {
		return nil, fmt.Errorf("parse min event_time: %w", err)
	}

	// upper bound = minEventTime + 2X (storage returns a slightly larger window;
	// GetAndFixedPendingEvents will only act on matches within 1s)
	upper := tmin.Add(2 * rangeInterval).UTC().Format(time.RFC3339Nano)
	lower := tmin.UTC().Format(time.RFC3339Nano)

	const query = `SELECT id, event_time, event_type, raw_event_type, dir_path, cmd_file, file_path, old_file_path FROM file_events WHERE processed = 0 AND event_time >= ? AND event_time <= ? ORDER BY event_time ASC`

	rows, err := s.db.Query(query, lower, upper)
	if err != nil {
		return nil, fmt.Errorf("query pending window: %w", err)
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
			cmdFile      sql.NullString
			filePath     string
			oldFilePath  sql.NullString
		)
		if err := rows.Scan(&id, &eventTimeStr, &eventType, &rawEventType, &dirPath, &cmdFile, &filePath, &oldFilePath); err != nil {
			return nil, fmt.Errorf("scan pending row: %w", err)
		}
		t, err := time.Parse(time.RFC3339Nano, eventTimeStr)
		if err != nil {
			return nil, fmt.Errorf("parse time: %w", err)
		}
		ev := Event{
			EventTime:    t,
			RawEventType: "",
			EventType:    EventType(eventType),
			DirPath:      dirPath,
			CmdFile:      "",
			FilePath:     filePath,
			OldFilePath:  "",
		}
		if rawEventType.Valid {
			ev.RawEventType = rawEventType.String
		}
		if cmdFile.Valid {
			ev.CmdFile = cmdFile.String
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

// MarkSkipped marks the event with given id as skipped (processed = 2).
func (s *Storage) MarkSkipped(id int64) error {
	const query = `UPDATE file_events SET processed = 2 WHERE id = ?`
	res, err := s.db.Exec(query, id)
	if err != nil {
		return fmt.Errorf("mark skipped exec: %w", err)
	}
	if ra, err := res.RowsAffected(); err == nil {
		if ra == 0 {
			return fmt.Errorf("mark skipped: no rows affected for id %d", id)
		}
	}
	return nil
}

// ConvertDeleteToMoveAndSkipCreate performs the conversion of a DELETE row to
// a MOVE and marks the create row as skipped (processed = 2) in a single
// transaction to ensure atomicity.
func (s *Storage) ConvertDeleteToMoveAndSkipCreate(deleteID int64, createID int64, oldFilePath, newFilePath string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	const updDelete = `UPDATE file_events SET event_type = ?, old_file_path = ?, file_path = ? WHERE id = ?`
	if res, err := tx.Exec(updDelete, string(EventType_MOVE), oldFilePath, newFilePath, deleteID); err != nil {
		tx.Rollback()
		return fmt.Errorf("convert delete->move exec: %w", err)
	} else {
		if ra, err := res.RowsAffected(); err == nil {
			if ra == 0 {
				tx.Rollback()
				return fmt.Errorf("convert delete->move: no rows affected for id %d", deleteID)
			}
		}
	}

	const skipCreate = `UPDATE file_events SET processed = 2 WHERE id = ?`
	if res, err := tx.Exec(skipCreate, createID); err != nil {
		tx.Rollback()
		return fmt.Errorf("mark create skipped exec: %w", err)
	} else {
		if ra, err := res.RowsAffected(); err == nil {
			if ra == 0 {
				tx.Rollback()
				return fmt.Errorf("mark create skipped: no rows affected for id %d", createID)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}
