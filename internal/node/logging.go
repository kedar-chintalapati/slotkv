package node

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"net"
	"slices"
	"strings"

	"github.com/hashicorp/go-hclog"
)

// newRaftLogger returns an hclog logger at level that forwards to l.
func newRaftLogger(l *slog.Logger, level string) hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{
		Name:        "raft",
		Level:       hclog.LevelFromString(level),
		Output:      slogWriter{l},
		JSONFormat:  true,
		DisableTime: true,
		Exclude:     closedConn,
	})
}

// closedConn drops the read error the transport logs when this node closes
// its own connections during shutdown.
func closedConn(_ hclog.Level, msg string, args ...any) bool {
	if msg != "failed to decode incoming command" {
		return false
	}
	for _, a := range args {
		if err, ok := a.(error); ok && errors.Is(err, net.ErrClosed) {
			return true
		}
	}
	return false
}

// slogWriter turns hclog's JSON lines into slog records.
type slogWriter struct{ l *slog.Logger }

func (w slogWriter) Write(p []byte) (int, error) {
	var m map[string]any
	if err := json.Unmarshal(p, &m); err != nil {
		w.l.Warn(strings.TrimSpace(string(p)))
		return len(p), nil
	}
	msg, _ := m["@message"].(string)
	level := slog.LevelInfo
	switch m["@level"] {
	case "trace":
		level = slog.LevelDebug - 4
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	args := []any{"module", m["@module"]}
	for _, k := range slices.Sorted(maps.Keys(m)) {
		if !strings.HasPrefix(k, "@") {
			args = append(args, k, m[k])
		}
	}
	w.l.Log(context.Background(), level, msg, args...)
	return len(p), nil
}
