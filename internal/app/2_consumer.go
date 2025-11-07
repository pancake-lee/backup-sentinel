package app

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
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

	// If a command file is provided, try to load per-event commands.
	if a.options.CmdFile != "" {
		if err := a.loadCmdFile(a.options.CmdFile); err != nil {
			plogger.Errorf("failed to load cmd file %s: %v", a.options.CmdFile, err)
			return fmt.Errorf("load cmd file: %w", err)
		}
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
	// choose command: prefer per-event mapping if present
	cmdTemplate := a.options.Cmd
	if a.options.Cmds != nil {
		if evCmd, ok := a.options.Cmds[pe.EventType]; ok && evCmd != "" {
			cmdTemplate = evCmd
		}
	}

	// If no template at all, error
	if cmdTemplate == "" {
		plogger.Errorf("no command configured to process events")
		return fmt.Errorf("no command configured")
	}

	// replace %fullfile% and execute it. Older behavior expected the word 'fullfile' then quoted path,
	// maintain compatibility: if template contains %fullfile% replace it, otherwise append ' fullfile <quoted>'
	var cmdStr string
	if strings.Contains(cmdTemplate, "%fullfile%") {
		// replace placeholder with a quoted path to preserve spaces
		cmdStr = strings.ReplaceAll(cmdTemplate, "%fullfile%", strconv.Quote(pe.FilePath))
	} else {
		cmdStr = cmdTemplate + " fullfile " + strconv.Quote(pe.FilePath)
	}
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

// loadCmdFile reads the provided JSON file and fills a.options.Cmds with
// normalized event type keys mapping to command templates. Expected JSON keys:
// add_cmd, modify_cmd, move_cmd, delete_cmd
func (a *App) loadCmdFile(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	var payload struct {
		AddCmd    string `json:"add_cmd"`
		ModifyCmd string `json:"modify_cmd"`
		MoveCmd   string `json:"move_cmd"`
		DeleteCmd string `json:"delete_cmd"`
	}
	if err := json.Unmarshal(b, &payload); err != nil {
		return fmt.Errorf("unmarshal cmd file: %w", err)
	}

	m := make(map[string]string)
	if payload.AddCmd != "" {
		m["CREATE"] = payload.AddCmd
	}
	if payload.ModifyCmd != "" {
		m["MODIFY"] = payload.ModifyCmd
	}
	if payload.MoveCmd != "" {
		m["RENAME"] = payload.MoveCmd
	}
	if payload.DeleteCmd != "" {
		m["DELETE"] = payload.DeleteCmd
	}

	a.options.Cmds = m
	plogger.Debugf("loaded commands from %s: %+v", path, a.options.Cmds)
	return nil
}
