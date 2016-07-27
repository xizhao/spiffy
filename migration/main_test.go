package migration

import (
	"os"
	"testing"

	"github.com/blendlabs/spiffy"
)

// TestMain is the testing entrypoint.
func TestMain(m *testing.M) {
	config := spiffy.NewDbConnectionFromEnvironment()
	spiffy.CreateDbAlias("main", config)
	spiffy.SetDefaultAlias("main")

	os.Exit(m.Run())
}
