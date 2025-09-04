// tracer/logger/logger.go
package logger

import (
	"encoding/json"

	"go.uber.org/zap"
)

var Log *Logger

type Logger struct {
	*zap.Logger
}

func InitLogger() {
	zapLogger, _ := zap.NewProduction()
	Log = &Logger{zapLogger}
}

func (l *Logger) Info(msg string, fields ...interface{}) {
	zapFields := make([]zap.Field, 0, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		if i+1 >= len(fields) {
			break
		}
		key, ok := fields[i].(string)
		if !ok {
			continue
		}
		switch v := fields[i+1].(type) {
		case string:
			if isJSON(v) {
				// For JSON strings, parse and use zap.Any
				zapFields = append(zapFields, zap.Any(key, parseJSON(v)))
			} else {
				zapFields = append(zapFields, zap.String(key, v))
			}
		case error:
			zapFields = append(zapFields, zap.Error(v))
		case int:
			zapFields = append(zapFields, zap.Int(key, v))
		case bool:
			zapFields = append(zapFields, zap.Bool(key, v))
		case []byte:
			zapFields = append(zapFields, zap.String(key, BytesToString(v)))
		case interface{}:
			zapFields = append(zapFields, zap.Any(key, v))
		}
	}
	l.Logger.Info(msg, zapFields...)
}

func (l *Logger) Error(msg string, fields ...interface{}) {
	zapFields := make([]zap.Field, 0, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		if i+1 >= len(fields) {
			break
		}
		key, ok := fields[i].(string)
		if !ok {
			continue
		}
		switch v := fields[i+1].(type) {
		case string:
			if isJSON(v) {
				// For JSON strings, parse and use zap.Any
				zapFields = append(zapFields, zap.Any(key, parseJSON(v)))
			} else {
				zapFields = append(zapFields, zap.String(key, v))
			}
		case error:
			zapFields = append(zapFields, zap.Error(v))
		case int:
			zapFields = append(zapFields, zap.Int(key, v))
		case bool:
			zapFields = append(zapFields, zap.Bool(key, v))
		case []byte:
			zapFields = append(zapFields, zap.String(key, BytesToString(v)))
		case interface{}:
			zapFields = append(zapFields, zap.Any(key, v))
		}
	}
	l.Logger.Error(msg, zapFields...)
}

// BytesToString safely converts a byte slice to string for logging
func BytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return string(b)
}

// isJSON checks if a string is valid JSON
func isJSON(s string) bool {
	var js json.RawMessage
	return json.Unmarshal([]byte(s), &js) == nil
}

// parseJSON parses a JSON string into an interface{}
func parseJSON(s string) interface{} {
	var v interface{}
	if err := json.Unmarshal([]byte(s), &v); err == nil {
		return v
	}
	return s
}
