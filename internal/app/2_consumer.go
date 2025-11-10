package app

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

	// If a command file is provided, try to load per-event commands.
	if a.options.CmdFile != "" {
		if err := a.loadCmdFile(a.options.CmdFile); err != nil {
			plogger.Errorf("failed to load cmd file %s: %v", a.options.CmdFile, err)
			return fmt.Errorf("load cmd file: %w", err)
		}
	}

	// If Check flag is set, just list once and exit.
	if a.options.Check {
		pending, err := st.GetPendingEvents()
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
		pending, err := GetAndFixedPendingEvents(st)
		if err != nil {
			plogger.Errorf("get pending: %v", err)
			return
		}

		for _, pe := range pending {
			if err := a.processPendingEvent(st, pe); err != nil {
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

// processPendingEvent handles a single PendingEvent
func (a *App) processPendingEvent(st *Storage, pe PendingEvent) error {
	plogger.Infof("process id=%d type=%s file=%s at=%s", pe.ID, pe.EventType, pe.FilePath, pe.EventTime.Format(time.RFC3339))
	// choose command: prefer per-event mapping if present
	cmdTemplate := a.options.Cmd
	if a.options.Cmds != nil {
		evCmd, ok := a.options.Cmds[pe.EventType]
		if ok && evCmd != "" {
			cmdTemplate = evCmd
		}
	}

	// If no template at all, error
	if cmdTemplate == "" {
		plogger.Errorf("no command configured to process events")
		return fmt.Errorf("no command configured")
	}

	// if template contains %fullfile% replace it, otherwise append ' fullfile <quoted>'
	var cmdStr string
	if strings.Contains(cmdTemplate, "%fullfile%") {
		// replace placeholder with a quoted path to preserve spaces
		cmdStr = strings.ReplaceAll(cmdTemplate, "%fullfile%", strconv.Quote(pe.FilePath))
	} else {
		cmdStr = cmdTemplate + " fullfile " + strconv.Quote(pe.FilePath)
	}
	out, err := putil.ExecSplit(cmdStr)
	plogger.Debugf("exec cmd[%s] err[%v] out[\n-----\n%v\n-----]", cmdStr, err, out)
	if err != nil {
		return plogger.LogErr(err)
	}

	err = st.MarkProcessed(pe.ID)
	if err != nil {
		plogger.Errorf("mark processed id=%d: %v", pe.ID, err)
		return plogger.LogErr(err)
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

	m := make(map[EventType]string)
	if payload.AddCmd != "" {
		m[EventType_CREATE] = payload.AddCmd
	}
	if payload.ModifyCmd != "" {
		m[EventType_MODIFY] = payload.ModifyCmd
	}
	if payload.MoveCmd != "" {
		m[EventType_MOVE] = payload.MoveCmd
	}
	if payload.DeleteCmd != "" {
		m[EventType_DELETE] = payload.DeleteCmd
	}

	a.options.Cmds = m
	plogger.Debugf("loaded commands from %s: %+v", path, a.options.Cmds)
	return nil
}

/*
	1：获取未处理的最早事件及其2s内的事件，但每次只处理1s内的事件
	2：重命名后面会紧接一个修改事件
		判定条件是：
			重命名和修改两个事件间隔1s之内，文件修改时间大于1s。
		判定重命名成功
			保留重命名事件，他将正常处理为一个MOVE事件
			丢弃这个修改事件，标记为跳过（processed = 2）
		判定失败：
			正常处理重命名事件，和1s后的修改事件。
	3：移动文件将产生“删除+新增”两个事件，我们将两个事件合并/修改为一个MOVE事件
		判定条件是：
			删除和新增两个事件间隔1s之内，文件修改时间大于1s。
		判定移动成功：
			把删除事件修改为MOVE事件，并且回写到数据库
				原del事件file_path字段是旧路径，
				新mv事件file_path字段是新路径，old_file_path字段是旧路径
				数据库有raw_event_type记录原始事件类型，不用担心丢失原始数据
			丢弃新增事件，标记为跳过（processed = 2）
		判定失败：
			正常处理删除事件和1s后的新增事件。
*/

// 调用st.GetPendingEvents，并且处理一些特殊事件的转换逻辑，再返回给外层处理
func GetAndFixedPendingEvents(st *Storage) ([]PendingEvent, error) {
	all, err := st.GetPendingEvents()
	if err != nil {
		return nil, err
	}
	if len(all) == 0 {
		return all, nil
	}

	earliestTime := all[0].EventTime

	var out []PendingEvent
	skip := make(map[int]bool)
	for i := 0; i < len(all); i++ {
		if skip[i] {
			// this index was consumed/marked skipped by an earlier merge
			continue
		}
		cur := all[i]
		if cur.EventTime.Sub(earliestTime) > time.Second {
			// beyond 1s window; stop processing further events
			break
		}

		// Attempt to detect DELETE + CREATE -> MOVE
		if cur.EventType == EventType_DELETE {
			// find a CREATE event within 1s matching the path semantics
			match := func(a, b PendingEvent) bool {
				return b.EventType == EventType_CREATE &&
					a.Size == b.Size &&
					filepath.Base(a.FilePath) == filepath.Base(b.FilePath)
			}
			idx := findNextMatchingIndex(all, i, match, time.Second)
			if idx != -1 {
				next := all[idx]
				if err := st.ConvertDeleteToMoveAndSkipCreate(cur.ID, next.ID, cur.FilePath, next.FilePath); err != nil {
					plogger.Errorf("convert delete->move tx id=%d create=%d: %v", cur.ID, next.ID, err)
				} else {
					plogger.Debugf("converted delete id=%d -> MOVE to %s and skipped create id=%d", cur.ID, next.FilePath, next.ID)
					cur.EventType = EventType_MOVE
					cur.OldFilePath = cur.FilePath
					cur.FilePath = next.FilePath
				}
				// mark the create index as skipped so it's not processed later in-memory
				skip[idx] = true
				out = append(out, cur)
				continue
			}
		}

		// Attempt to detect MOVE followed by MODIFY to skip the MODIFY
		if cur.EventType == EventType_MOVE {
			match := func(a, b PendingEvent) bool {
				return b.EventType == EventType_MODIFY &&
					b.FilePath == a.FilePath
			}
			idx := findNextMatchingIndex(all, i, match, time.Second)
			if idx != -1 {
				next := all[idx]
				// mark DB and mark in-memory to skip when loop reaches it
				if err := st.MarkSkipped(next.ID); err != nil {
					plogger.Errorf("mark skipped id=%d: %v", next.ID, err)
				} else {
					plogger.Debugf("marked skipped id=%d", next.ID)
				}
				skip[idx] = true
				out = append(out, cur)
				continue
			}
		}

		// otherwise, normal event => process
		out = append(out, cur)
	}

	return out, nil
}

func findNextMatchingIndex(all []PendingEvent, start int, match func(a, b PendingEvent) bool, maxDelta time.Duration) int {
	base := all[start]
	for j := start + 1; j < len(all); j++ {
		dt := all[j].EventTime.Sub(base.EventTime).Abs()
		if dt > maxDelta {
			// beyond search window
			break
		}
		if match(base, all[j]) {
			return j
		}
	}
	return -1
}
