package migration

import (
	"fmt"
	"strings"

	logger "github.com/blendlabs/go-logger"
	"github.com/blendlabs/go-util"
)

const (
	// EventFlagMigration is a logger event flag.
	EventFlagMigration logger.EventFlag = "db.migration"
)

// NewLogger returns a new logger instance.
func NewLogger() *Logger {
	return &Logger{
		Output: logger.NewFromEnvironment(),
	}
}

// NewLoggerFromAgent returns a new logger instance from an existing logger..
func NewLoggerFromAgent(agent *logger.Agent) *Logger {
	return &Logger{
		Output: agent,
	}
}

// Logger is a logger for migration steps.
type Logger struct {
	Output *logger.Agent
	Phase  string // `test` or `apply`
	Result string // `apply` or `skipped` or `failed`

	applied int
	skipped int
	failed  int
}

// Applyf active actions to the log.
func (l *Logger) Applyf(m Migration, body string, args ...interface{}) error {
	l.applied = l.applied + 1
	l.Result = "applied"
	l.write(m, logger.ColorLightGreen, fmt.Sprintf(body, args...))
	return nil
}

// Skipf passive actions to the log.
func (l *Logger) Skipf(m Migration, body string, args ...interface{}) error {
	l.skipped = l.skipped + 1
	l.Result = "skipped"
	l.write(m, logger.ColorGreen, fmt.Sprintf(body, args...))
	return nil
}

// Errorf writes errors to the log.
func (l *Logger) Errorf(m Migration, err error) error {
	l.failed = l.failed + 1
	l.Result = "failed"
	l.write(m, logger.ColorRed, fmt.Sprintf("%v", err.Error()))
	return err
}

// WriteStats writes final stats to output
func (l *Logger) WriteStats() {
	l.Output.Writer().Printf("\n\t%s applied %s skipped %s failed\n\n",
		l.Output.Writer().Colorize(util.String.IntToString(l.applied), logger.ColorGreen),
		l.Output.Writer().Colorize(util.String.IntToString(l.skipped), logger.ColorLightGreen),
		l.Output.Writer().Colorize(util.String.IntToString(l.failed), logger.ColorRed),
	)
}

func (l *Logger) colorize(text string, color logger.AnsiColorCode) string {
	return l.Output.Writer().Colorize(text, color)
}

func (l *Logger) colorizeFixedWidthLeftAligned(text string, color logger.AnsiColorCode, width int) string {
	fixedToken := fmt.Sprintf("%%-%ds", width)
	fixedMessage := fmt.Sprintf(fixedToken, text)
	return l.Output.Writer().Colorize(fixedMessage, color)
}

func (l *Logger) write(m Migration, color logger.AnsiColorCode, body string) {
	if l.Output == nil {
		return
	}

	resultColor := logger.ColorBlue
	switch l.Result {
	case "skipped":
		resultColor = logger.ColorYellow
	case "failed":
		resultColor = logger.ColorRed
	}

	l.Output.Eventf(
		EventFlagMigration,
		"%s%s %s %s %s %s %s %s",
		l.colorize("migrate", logger.ColorBlue),
		l.colorizeFixedWidthLeftAligned(l.Phase, logger.ColorBlue, 5),
		l.colorize("--", logger.ColorLightBlack),
		l.colorizeFixedWidthLeftAligned(l.Result, resultColor, 5),
		l.renderStack(m, color),
		l.colorize("--", logger.ColorLightBlack),
		body,
	)
}

func (l *Logger) renderStack(m Migration, color logger.AnsiColorCode) string {
	stackSeparator := fmt.Sprintf(" %s ", l.colorize(">", logger.ColorLightBlack))
	var renderedStack string
	cursor := m.Parent()
	for cursor != nil {
		if len(cursor.Label()) > 0 {
			renderedStack = stackSeparator + cursor.Label() + renderedStack
		}
		cursor = cursor.Parent()
	}
	return strings.TrimPrefix(renderedStack, " ")
}
