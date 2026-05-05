//go:build unit

package config

import (
	"strings"
	"testing"
)

// FuzzSplitCSV exercises the CSV splitter used by LoadConfig to parse
// STREAMING_BROKERS.
func FuzzSplitCSV(f *testing.F) {
	f.Add("")
	f.Add("one")
	f.Add("one,two,three")
	f.Add("   whitespace  ")
	f.Add("a,,b")
	f.Add("a, b, c")
	f.Add("trailing,")
	f.Add(",leading")
	f.Add("embedded,spaces, only")
	f.Add("tenant-α,tenant-β")
	f.Add("a\n,b\r,c\t")
	f.Add(strings.Repeat("x", 4096) + ",tail")

	f.Fuzz(func(t *testing.T, in string) {
		if len(in) > 64*1024 {
			t.Skip("input exceeds bounded parser fuzz size")
		}

		got := splitCSV(in)

		for i, elem := range got {
			if elem == "" {
				t.Fatalf("splitCSV(%q)[%d] is empty; invariant: no empty strings", in, i)
			}

			if strings.TrimSpace(elem) != elem {
				t.Fatalf("splitCSV(%q)[%d] = %q; invariant: every element is trimmed", in, i, elem)
			}
		}
	})
}
