package app

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// parsedCmds stores mapping from EventType to command template for a file.
type parsedCmds map[EventType]string

type cmdFileEntry struct {
	parsed  parsedCmds
	expires time.Time
}

// CmdFileManager caches parsed cmd_file mappings and provides lookup by event type.
type CmdFileManager struct {
	mu    sync.Mutex
	cache map[string]*cmdFileEntry
	ttl   time.Duration
}

// NewCmdFileManager constructs a manager with 5 minute TTL by default.
func NewCmdFileManager(ttl time.Duration) *CmdFileManager {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &CmdFileManager{cache: make(map[string]*cmdFileEntry), ttl: ttl}
}

// loadAndParse reads file and returns parsed mapping.
func loadAndParse(path string) (parsedCmds, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cmd file %s: %w", path, err)
	}
	var payload struct {
		AddCmd    string `json:"add_cmd"`
		ModifyCmd string `json:"modify_cmd"`
		RenameCmd string `json:"rename_cmd"`
		MoveCmd   string `json:"move_cmd"`
		DeleteCmd string `json:"delete_cmd"`
	}
	if err := json.Unmarshal(b, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal cmd file %s: %w", path, err)
	}
	m := make(parsedCmds)
	if payload.AddCmd != "" {
		m[EventType_CREATE] = payload.AddCmd
	}
	if payload.ModifyCmd != "" {
		m[EventType_MODIFY] = payload.ModifyCmd
	}
	if payload.RenameCmd != "" {
		m[EventType_RENAME] = payload.RenameCmd
	}
	if payload.MoveCmd != "" {
		m[EventType_MOVE] = payload.MoveCmd
	}
	if payload.DeleteCmd != "" {
		m[EventType_DELETE] = payload.DeleteCmd
	}
	return m, nil
}

// Load loads and caches the parsed mapping for the given path.
func (m *CmdFileManager) Load(path string) error {
	if path == "" {
		return nil
	}
	parsed, err := loadAndParse(path)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.cache[path] = &cmdFileEntry{parsed: parsed, expires: time.Now().Add(m.ttl)}
	m.mu.Unlock()
	return nil
}

// GetCmd returns the command template for given file path and event type.
// It will read and cache the file if necessary.
func (m *CmdFileManager) GetCmd(path string, ev EventType) (string, error) {
	if path == "" {
		return "", nil
	}
	m.mu.Lock()
	e, ok := m.cache[path]
	if ok && time.Now().Before(e.expires) {
		if cmd, ok := e.parsed[ev]; ok {
			m.mu.Unlock()
			return cmd, nil
		}
		m.mu.Unlock()
		return "", nil
	}
	m.mu.Unlock()

	parsed, err := loadAndParse(path)
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	m.cache[path] = &cmdFileEntry{parsed: parsed, expires: time.Now().Add(m.ttl)}
	m.mu.Unlock()
	if parsed == nil {
		return "", nil
	}
	if cmd, ok := parsed[ev]; ok {
		return cmd, nil
	}
	return "", nil
}

// PurgeExpired removes expired entries; called optionally by callers.
func (m *CmdFileManager) PurgeExpired() {
	now := time.Now()
	m.mu.Lock()
	for k, v := range m.cache {
		if now.After(v.expires) {
			delete(m.cache, k)
		}
	}
	m.mu.Unlock()
}
