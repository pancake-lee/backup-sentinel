package app

import (
	"errors"
	"fmt"
	"strings"

	"github.com/pancake-lee/pgo/pkg/plogger"

	"backup-sentinel/internal/monitor"
)

// Mode represents the operating mode for the executable.
type Mode int

const (
	// ModeProducer represents the Directory Monitor triggered producer mode.
	ModeProducer Mode = iota
	// ModeConsumer represents the long running consumer mode.
	ModeConsumer
)

// String returns a human readable label.
func (m Mode) String() string {
	switch m {
	case ModeConsumer:
		return "consumer"
	default:
		return "producer"
	}
}

// Options configures the application runtime.
type Options struct {
	Mode Mode
}

// App coordinates the executable lifecycle.
type App struct {
	options Options
}

// New constructs an App instance with defaults.
func New(options Options) *App {
	return &App{options: options}
}

// Run executes the requested workflow. For now it only parses Directory Monitor payloads.
func (a *App) Run(args []string) error {
	plogger.Debugf("starting in %s mode", a.options.Mode)

	if len(args) == 0 {
		return errors.New("missing Directory Monitor payload")
	}
	for i, arg := range args {
		plogger.Debugf("arg[%d]: %s", i, arg)
	}
	rawPayload := strings.Join(args, " ")
	rawPayload = strings.ReplaceAll(rawPayload, "\\", "\\\\")
	if !strings.Contains(rawPayload, `""""`) &&
		strings.Contains(rawPayload, `"""`) {
		rawPayload = strings.ReplaceAll(rawPayload, `"""`, `""`)
	}
	plogger.Debugf("raw payload: %s", rawPayload)

	if shouldSkipPath(rawPayload) {
		plogger.Debugf("skipping event for path matching skip patterns")
		return nil
	}

	event, err := monitor.ParseDirectoryMonitorPayload(rawPayload)
	if err != nil {
		return fmt.Errorf("parse Directory Monitor payload: %w", err)
	}

	plogger.Debugf("event ready for downstream processing: event_type=%s dir=%s file=%s at=%s", event.EventType, event.DirPath, event.FilePath, event.EventTime.Format("2006-01-02T15:04:05"))

	// TODO: enqueue or persist event in SQLite when running as producer.
	if a.options.Mode == ModeProducer {
		// TODO: insert event into file_events table using WAL-enabled SQLite connection pool.
	}

	// TODO: implement asynchronous consumer pipeline for uploads and delete notifications.
	if a.options.Mode == ModeConsumer {
		// TODO: evaluate latest pending event per file and dispatch to upload service.
	}

	return nil
}

var skipPatterns = []string{
	"@eaDir",
	"@SynoEAStream",
	"._",
	".DS_Store",
	"Thumbs.db",
}

func shouldSkipPath(filePath string) bool {
	for _, pattern := range skipPatterns {
		if strings.Contains(filePath, pattern) {
			return true
		}
	}
	return false
}
