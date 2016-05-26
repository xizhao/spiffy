package migration

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/blendlabs/go-util"
)

// NewLogger returns a new logger instance.
func NewLogger() *Logger {
	return &Logger{
		Output: log.New(os.Stdout, util.Color("migrate", util.ColorBlue), 0x0),
	}
}

// NewLoggerFromLog returns a new logger instance from an existing logger..
func NewLoggerFromLog(l *log.Logger) *Logger {
	return &Logger{
		Output: l,
	}
}

// Logger is a logger for migration steps.
type Logger struct {
	Output *log.Logger
	Phase  string // `test` or `apply`

	applied int
	skipped int
	failed  int
}

// Applyf active actions to the log.
func (l *Logger) Applyf(stack []string, body string, args ...interface{}) error {
	l.applied++
	l.write(stack, util.ColorLightGreen, fmt.Sprintf(body, args...))
	return nil
}

// Skipf passive actions to the log.
func (l *Logger) Skipf(stack []string, body string, args ...interface{}) error {
	l.skipped++
	l.write(stack, util.ColorGreen, fmt.Sprintf(body, args...))
	return nil
}

// Errorf writes errors to the log.
func (l *Logger) Errorf(stack []string, err error) error {
	l.failed++
	l.write(stack, util.ColorRed, fmt.Sprintf("%v", err.Error()))
	return err
}

// WriteStats writes final stats to output
func (l *Logger) WriteStats() {
	l.Output.Printf("\n\t%s applied %s skipped %s failed\n",
		util.Color(util.IntToString(l.applied), util.ColorGreen),
		util.Color(util.IntToString(l.applied), util.ColorLightGreen),
		util.Color(util.IntToString(l.applied), util.ColorRed),
	)
}

func (l *Logger) write(stack []string, color, body string) {
	l.Output.Printf(" %s %s %s %s %s",
		util.ColorFixedWidthLeftAligned(l.Phase, util.ColorBlue, 5),
		util.Color("--", util.ColorLightBlack),
		util.ColorFixedWidthLeftAligned(strings.Join(stack, util.Color(" > ", util.ColorLightBlack)), color, 15),
		util.Color("--", util.ColorLightBlack),
		body,
	)
}
