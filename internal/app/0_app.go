package app

import (
	"github.com/pancake-lee/pgo/pkg/plogger"
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
	// Check when running as consumer: if true, only print pending events instead of processing.
	Check bool
	// DBPath specifies the path to the sqlite database file. If empty, caller should
	// provide a default (e.g. ./backupSentinel.db).
	DBPath string
	// Cmd is an optional command template executed to process an event. It may
	// contain %fullfile% which will be replaced with the event's FilePath.
	Cmd string
	// CmdFile optionally points to a JSON file containing per-event commands.
	// When set, the file is parsed and commands loaded into Cmds.
	CmdFile string
	// Cmds stores commands keyed by normalized event type (e.g. CREATE, MODIFY, RENAME, DELETE).
	Cmds map[EventType]string
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
	if a.options.Mode == ModeConsumer {
		return a.runConsumer()
	}
	return a.runProducer(args)
}
