package app

import (
	"os"
	"path/filepath"
	"strings"

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
}

// defaultDBPath returns a path for the sqlite DB derived from the executable name
// (same name as the executable, with .db suffix, placed next to the executable).
func defaultDBPath() string {
	exe, err := os.Executable()
	if err != nil {
		// fallback to ./backupsentinel.db
		return "./backupsentinel.db"
	}
	dir := filepath.Dir(exe)
	base := filepath.Base(exe)
	name := strings.TrimSuffix(base, filepath.Ext(base))
	return filepath.Join(dir, name+".db")
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
