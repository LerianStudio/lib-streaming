//go:build unit

package buildmeta

import (
	"bufio"
	"os"
	"strings"
	"testing"
)

// TestModulePath_MatchesGoMod ties the hardcoded modulePath to the module
// declared in go.mod. A major bump that edits go.mod without editing the
// constant would make every emitted span claim the previous major; this test
// is the tripwire for that.
func TestModulePath_MatchesGoMod(t *testing.T) {
	f, err := os.Open("../../go.mod")
	if err != nil {
		t.Fatalf("open go.mod: %v", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "module ") {
			continue
		}

		if got := strings.TrimSpace(strings.TrimPrefix(line, "module ")); got != modulePath {
			t.Fatalf("go.mod declares module %q, buildmeta.modulePath is %q", got, modulePath)
		}

		return
	}

	t.Fatal("go.mod has no module line")
}
