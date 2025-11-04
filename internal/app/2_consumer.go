package app

import (
	"fmt"
	"time"

	"github.com/pancake-lee/pgo/pkg/plogger"
)

func (a *App) runConsumer() error {
	dbPath := defaultDBPath()
	st, err := OpenAndInit(dbPath)
	if err != nil {
		plogger.Errorf("open sqlite db %s: %v", dbPath, err)
		return fmt.Errorf("open db: %w", err)
	}
	defer st.Close()

	pending, err := st.GetPendingEvents(10)
	if err != nil {
		plogger.Errorf("get pending: %v", err)
		return fmt.Errorf("get pending: %w", err)
	}
	if a.options.Check {
		for _, pe := range pending {
			plogger.Infof("pending id=%d type=%s file=%s at=%s", pe.ID, pe.EventType, pe.FilePath, pe.EventTime.Format(time.RFC3339))
		}
		return nil
	}

	// TODO: actually process pending events (upload/delete)
	return nil
}
