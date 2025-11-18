package app

import (
	"testing"
	"time"
)

func TestParseDirectoryMonitorPayload(t *testing.T) {
	// Based on the example: {"t":"2025/11/3 16:43:40", "e":"删除", "d":"\\192.168.1.2\1\a\b\c", "f":"\\192.168.1.2\1\a\b\c\1.jpg"}
	// Note: Backslashes in the JSON string must be escaped.
	const payload = `{"t":"2025/11/3 16:43:40", "e":"删除", "d":"\\\\192.168.1.2\\a\\b\\c", "f":"\\\\192.168.1.2\\a\\b\\c\\1.jpg"}`

	event, err := ParseDirectoryMonitorPayload(payload)
	if err != nil {
		t.Fatalf("ParseDirectoryMonitorPayload() error = %v", err)
	}

	expectedTime, _ := time.ParseInLocation(timestampLayout, "2025/11/3 16:43:40", time.Local)
	if !event.EventTime.Equal(expectedTime) {
		t.Errorf("Expected EventTime to be %v, got %v", expectedTime, event.EventTime)
	}

	if event.EventType != "DELETE" {
		t.Errorf("Expected EventType to be 'DELETE', got %q", event.EventType)
	}

	expectedDir := `\\192.168.1.2\a\b\c`
	if event.DirPath != expectedDir {
		t.Errorf("Expected DirPath to be %q, got %q", expectedDir, event.DirPath)
	}

	expectedFile := `\\192.168.1.2\a\b\c\1.jpg`
	if event.FilePath != expectedFile {
		t.Errorf("Expected FilePath to be %q, got %q", expectedFile, event.FilePath)
	}

	if event.RawEventType != "删除" {
		t.Errorf("Expected RawEventType to be '删除', got %q", event.RawEventType)
	}
}
