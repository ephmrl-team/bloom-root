package logger

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const FileName = "bloom-root.log"

const (
	InfoEmoji    = "ℹ️"
	SuccessEmoji = "✅"
	WarningEmoji = "⚠️"
	ErrorEmoji   = "❌"
)

type Logger struct {
	mu   sync.Mutex
	file *os.File
}

var defaultLogger = &Logger{}

func Init(storagePath string) error {
	if storagePath == "" {
		return errors.New("storage path is empty")
	}

	if err := os.MkdirAll(storagePath, 0o700); err != nil {
		return err
	}

	file, err := os.OpenFile(filepath.Join(storagePath, FileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}

	defaultLogger.mu.Lock()
	previous := defaultLogger.file
	defaultLogger.file = file
	defaultLogger.mu.Unlock()

	if previous != nil {
		_ = previous.Close()
	}

	Info("logger", "logging initialized", "file", FileName)

	return nil
}

func Close() error {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()

	if defaultLogger.file == nil {
		return nil
	}

	line := formatLine(SuccessEmoji, "logger", "logging stopped")
	_, _ = defaultLogger.file.WriteString(line)

	err := defaultLogger.file.Close()
	defaultLogger.file = nil

	return err
}

func Info(component, message string, fields ...any) {
	defaultLogger.write(InfoEmoji, component, message, fields...)
}

func Success(component, message string, fields ...any) {
	defaultLogger.write(SuccessEmoji, component, message, fields...)
}

func Warn(component, message string, fields ...any) {
	defaultLogger.write(WarningEmoji, component, message, fields...)
}

func Error(component, message string, fields ...any) {
	defaultLogger.write(ErrorEmoji, component, message, fields...)
}

func (l *Logger) write(emoji, component, message string, fields ...any) {
	line := formatLine(emoji, component, message, fields...)

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return
	}

	_, _ = l.file.WriteString(line)
}

func formatLine(emoji, component, message string, fields ...any) string {
	var builder strings.Builder

	builder.WriteString(time.Now().UTC().Format("2006-01-02T15:04:05.000Z"))
	builder.WriteString(" ")
	builder.WriteString(emoji)
	builder.WriteString(" [")
	builder.WriteString(sanitize(component))
	builder.WriteString("] ")
	builder.WriteString(sanitize(message))

	for i := 0; i+1 < len(fields); i += 2 {
		builder.WriteString(" ")
		builder.WriteString(sanitize(fmt.Sprint(fields[i])))
		builder.WriteString("=")
		builder.WriteString(formatValue(fields[i+1]))
	}

	builder.WriteString("\n")

	return builder.String()
}

func formatValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return "<nil>"
	case string:
		return sanitize(typed)
	case []byte:
		return fmt.Sprintf("<%d bytes>", len(typed))
	case error:
		return sanitize(typed.Error())
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	case time.Duration:
		return typed.String()
	case bool:
		return fmt.Sprint(typed)
	case int, int8, int16, int32, int64:
		return fmt.Sprint(typed)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprint(typed)
	case float32, float64:
		return fmt.Sprint(typed)
	default:
		return sanitize(fmt.Sprint(typed))
	}
}

func sanitize(value string) string {
	value = strings.ReplaceAll(value, "\n", "\\n")
	value = strings.ReplaceAll(value, "\r", "\\r")

	if index := strings.IndexByte(value, '?'); index >= 0 {
		end := len(value)
		if space := strings.IndexAny(value[index:], " \t"); space >= 0 {
			end = index + space
		}
		value = value[:index] + "?[redacted]" + value[end:]
	}

	value = redact(value, "token")
	value = redact(value, "recovery_key")
	value = redact(value, "signature")

	if len(value) > 512 {
		value = value[:512] + "..."
	}

	return value
}

func redact(value, key string) string {
	lowerValue := strings.ToLower(value)
	lowerKey := strings.ToLower(key)
	searchFrom := 0

	for {
		index := strings.Index(lowerValue[searchFrom:], lowerKey+"=")
		if index < 0 {
			return value
		}
		index += searchFrom

		start := index + len(key) + 1
		end := start
		for end < len(value) && value[end] != ' ' && value[end] != '\t' && value[end] != '&' && value[end] != ',' {
			end++
		}

		value = value[:start] + "***" + value[end:]
		lowerValue = strings.ToLower(value)
		searchFrom = start + 3
	}
}
