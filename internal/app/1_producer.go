package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/pancake-lee/pgo/pkg/plogger"
)

func (a *App) runProducer(args []string) error {
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

	// --------------------------------------------------
	event, err := ParseDirectoryMonitorPayload(rawPayload)
	if err != nil {
		return fmt.Errorf("parse Directory Monitor payload: %w", err)
	}

	// log event as JSON at info level
	if b, err := json.Marshal(event); err != nil {
		plogger.Infof("%+v", event)
	} else {
		plogger.Infof("%s", string(b))
	}

	// --------------------------------------------------
	// persist event into SQLite
	dbPath := a.options.DBPath
	if dbPath == "" {
		dbPath = "./backupSentinel.db"
	}
	st, err := OpenAndInit(dbPath)
	if err != nil {
		plogger.Errorf("open sqlite db %s: %v", dbPath, err)
		return fmt.Errorf("open db: %w", err)
	}
	defer st.Close()

	// --------------------------------------------------
	id, err := st.InsertEvent(event)
	if err != nil {
		plogger.Errorf("insert event: %v", err)
		return fmt.Errorf("insert event: %w", err)
	}
	plogger.Debugf("persisted event id=%d", id)

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
