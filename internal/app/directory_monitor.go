package app

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pancake-lee/pgo/pkg/plogger"
	"github.com/pancake-lee/pgo/pkg/putil"
)

// 配置: {"t":"%date% %time%", "e":"%event%", "d":"%dirpath%", "f":"%fullfile%"}
// 配置: -t "%date% %time%" -e "%event%" -d "%dirpath%" -f "%fullfile%"
// 以上两种配置都难处理，因为路径包含空格，因为win路径的"\"
// Directory Monitor 提供的变量并不会主动添加转义，则输入的字符串是[\a\b\c]，但即使包围了引号也无法正确解析

// 下面的配置，再附加以下3个处理，可以得到正确的结果：
// 配置: "{\"t\":\"%date% %time%\", \"e\":\"%event%\", \"d\":\"%dirpath%\", \"f\":\"%fullfile%\", \"of\":\"%oldfullfile%\"}"
// 处理1：路径包含空格：rawPayload := strings.Join(args, " ")
// 处理2：rawPayload = strings.ReplaceAll(rawPayload, "\\", "\\\\")
// 处理3：如果出现多余的引号，则替换掉，如果of值为空，不知道为什么给我拼来连续3个引号["""]
// 示例: {"t":"2025/11/3 16:43:40", "e":"删除", "d":"\\\\192.168.1.2\\a\\b\\c", "f":"\\\\192.168.1.2\\a\\b\\c\\1.jpg","of":"\\\\192.168.1.2\\a\\b\\c\\11.jpg"}

// --------------------------------------------------
// 输入相关定义
// --------------------------------------------------
const timestampLayout = "2006/1/2 15:04:05"

type jsonPayload struct {
	Timestamp string `json:"t"`
	EventType string `json:"e"`
	Directory string `json:"d"`
	File      string `json:"f"`
	OldFile   string `json:"of"`
	CmdFile   string `json:"cmd_file"`
	Size      string `json:"s"`
}

var eventTypeMap = map[string]EventType{
	"新增":   EventType_CREATE,
	"创建":   EventType_CREATE,
	"修改":   EventType_MODIFY,
	"重命名":  EventType_RENAME,
	"删除":   EventType_DELETE,
	"MOVE": EventType_MOVE,
}

// --------------------------------------------------
// DB或输出相关定义
// --------------------------------------------------
type EventType string

// move比rename更加合适，类似linux中mv命令
const (
	EventType_CREATE EventType = "CREATE"
	EventType_MODIFY EventType = "MODIFY"
	EventType_RENAME EventType = "RENAME"
	EventType_MOVE   EventType = "MOVE"
	EventType_DELETE EventType = "DELETE"
)

type Event struct {
	EventTime    time.Time
	RawEventType string
	EventType    EventType
	DirPath      string
	FilePath     string
	OldFilePath  string
	CmdFile      string
	Size         int64
}

// --------------------------------------------------
// ParseDirectoryMonitorPayload converts the raw JSON string argument into a structured Event.
func ParseDirectoryMonitorPayload(raw string) (*Event, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("empty Directory Monitor payload")
	}

	var payload jsonPayload
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		plogger.Errorf("failed to unmarshal json payload: %v", err)
		return nil, fmt.Errorf("unmarshal json: %w", err)
	}

	occurredAt, err := time.ParseInLocation(timestampLayout, payload.Timestamp, time.Local)
	if err != nil {
		plogger.Errorf("failed to parse timestamp %q: %v", payload.Timestamp, err)
		return nil, fmt.Errorf("parse timestamp %q: %w", payload.Timestamp, err)
	}

	normalizedType, ok := eventTypeMap[payload.EventType]
	if !ok {
		plogger.Errorf("unmapped event type %q, using uppercase version %q", payload.EventType, normalizedType)
		return nil, fmt.Errorf("unmapped event type %q", payload.EventType)
	}

	event := Event{
		EventTime:    occurredAt,
		RawEventType: payload.EventType,
		EventType:    normalizedType,
		DirPath:      payload.Directory,
		FilePath:     payload.File,
		OldFilePath:  payload.OldFile,
		CmdFile:      payload.CmdFile,
		Size:         putil.StrToInt64WithDefault(payload.Size, 0),
	}

	return &event, nil
}
