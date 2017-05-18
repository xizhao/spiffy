package migration

import "sync"

var (
	defaultSuite     = &Suite{}
	defaultSuiteLock sync.Mutex
)

// Register adds a process to the default suite.
func Register(m ...Migration) error {
	defaultSuiteLock.Lock()
	defer defaultSuiteLock.Unlock()
	defaultSuite.addMigrations(m...)
	return nil
}

// Run passes the default suite to the handler method. It acquires a read lock wrapping the action.
func Run(handler func(Migration) error) error {
	defaultSuiteLock.Lock()
	defer defaultSuiteLock.Unlock()
	return handler(defaultSuite)
}

// Default returns the default migration suite.
func Default() Migration {
	return defaultSuite
}
