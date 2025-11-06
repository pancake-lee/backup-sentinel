package app

import (
	"fmt"
	"strings"
	"time"

	"github.com/pancake-lee/pgo/pkg/plogger"
	"github.com/pancake-lee/pgo/pkg/putil"
)

func (a *App) runConsumer() error {
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

	// use named method to process a pending event
	process := func(pe PendingEvent) error {
		return a.processPendingEvent(st, pe)
	}

	// If Check flag is set, just list once and exit.
	if a.options.Check {
		pending, err := st.GetPendingEvents(10)
		if err != nil {
			plogger.Errorf("get pending: %v", err)
			return fmt.Errorf("get pending: %w", err)
		}
		for _, pe := range pending {
			plogger.Infof("pending id=%d type=%s file=%s at=%s", pe.ID, pe.EventType, pe.FilePath, pe.EventTime.Format(time.RFC3339))
		}
		return nil
	}

	// Continuous processing loop: check DB every 1s, but ensure that if processing
	// of messages takes longer than the interval we don't run overlapping cycles.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Run an initial immediate check
	runOnce := func() {
		pending, err := st.GetPendingEvents(10)
		if err != nil {
			plogger.Errorf("get pending: %v", err)
			return
		}
		for _, pe := range pending {
			if err := process(pe); err != nil {
				// log and continue with next pending event
				plogger.Errorf("processing id=%d failed: %v", pe.ID, err)
			}
		}
	}

	// Start background loop; block until context cancellation not provided here so run until program exit.
	for {
		runOnce()
		// wait for next tick
		<-ticker.C
	}
}

// processPendingEvent handles a single PendingEvent: it logs the action and
// marks the row as processed on success. Separated for easier testing and
// future extension (actual upload/delete logic).
func (a *App) processPendingEvent(st *Storage, pe PendingEvent) error {
	plogger.Infof("process id=%d type=%s file=%s at=%s", pe.ID, pe.EventType, pe.FilePath, pe.EventTime.Format(time.RFC3339))

	// replace %fullfile% and execute it.
	cmdStr := a.options.Cmd
	cmdStr = strings.ReplaceAll(cmdStr, `%fullfile%`, pe.FilePath)
	out, err := putil.ExecSplit(cmdStr)
	plogger.Debugf("exec cmd[%s]\nerr[%v]\nout[%v]", cmdStr, err, out)
	if err != nil {
		return plogger.LogErr(err)
	}

	if err := st.MarkProcessed(pe.ID); err != nil {
		plogger.Errorf("mark processed id=%d: %v", pe.ID, err)
		return fmt.Errorf("mark processed: %w", err)
	}
	plogger.Debugf("marked processed id=%d", pe.ID)
	return nil
}
