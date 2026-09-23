//go:build unit

package transport

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// The bytes a Kafka header can carry that a text consumer cannot hold.
//
// Every case is expressed as bytes a real producer can emit, never as a Go
// string literal the compiler already validated, because the whole point is
// that nothing between the producer and this function checks anything.
func TestSanitizeHeaderValue_HostileBytesBecomeStorableText(t *testing.T) {
	t.Parallel()

	for name, testCase := range map[string]struct {
		input []byte
		want  string
	}{
		"clean value is returned unchanged": {
			input: []byte("11111111-1111-1111-1111-111111111111"),
			want:  "11111111-1111-1111-1111-111111111111",
		},
		"clean multi-byte value is returned unchanged": {
			input: []byte("contrato consignado — parcela 3"),
			want:  "contrato consignado — parcela 3",
		},
		"empty value is returned unchanged": {
			input: []byte(""),
			want:  "",
		},
		"trailing NUL": {
			input: []byte("tenant-a\x00"),
			want:  "tenant-a" + HeaderValueReplacement,
		},
		"embedded NUL": {
			input: []byte("tenant\x00a"),
			want:  "tenant" + HeaderValueReplacement + "a",
		},
		"NUL alone": {
			input: []byte("\x00"),
			want:  HeaderValueReplacement,
		},
		"invalid sequence": {
			input: []byte{'g', 'w', 0xff, 0xfe},
			want:  "gw" + HeaderValueReplacement,
		},
		"truncated multi-byte rune": {
			input: []byte{0xc3},
			want:  HeaderValueReplacement,
		},
		"surrogate half, which is not valid UTF-8": {
			input: []byte{'a', 0xed, 0xa0, 0x80, 'b'},
			want:  "a" + HeaderValueReplacement + "b",
		},
		"both classes in one value": {
			input: []byte{'t', 0x00, 'e', 0xff, 'n'},
			want:  "t" + HeaderValueReplacement + "e" + HeaderValueReplacement + "n",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := SanitizeHeaderValue(testCase.input)

			if got != testCase.want {
				t.Fatalf("SanitizeHeaderValue(%q) = %q, want %q", testCase.input, got, testCase.want)
			}

			if strings.IndexByte(got, 0) >= 0 {
				t.Fatalf("SanitizeHeaderValue(%q) kept a NUL: PostgreSQL refuses it in TEXT permanently", testCase.input)
			}

			if !utf8.ValidString(got) {
				t.Fatalf("SanitizeHeaderValue(%q) = %q is not valid UTF-8", testCase.input, got)
			}
		})
	}
}

// The replacement must be VISIBLE. Dropping the offending bytes would turn a
// hostile value into a clean-looking one, so nobody reading the stored value
// could tell that the writer sent something the column refused.
func TestSanitizeHeaderValue_ReplacesRatherThanDeletes(t *testing.T) {
	t.Parallel()

	got := SanitizeHeaderValue([]byte("before\x00after"))

	if !strings.Contains(got, HeaderValueReplacement) {
		t.Fatalf("SanitizeHeaderValue = %q, want a visible replacement rune", got)
	}

	if got == "beforeafter" {
		t.Fatal("the bytes were deleted, so a scrubbed value now reads as an original one")
	}
}

// NUL is replaced BEFORE the UTF-8 pass, not after. U+0000 is valid UTF-8, so
// strings.ToValidUTF8 leaves it in place: a function that ran only that pass
// would return a string PostgreSQL still refuses, and would look correct in
// every test that fed it invalid bytes rather than a NUL.
func TestSanitizeHeaderValue_NULSurvivesAnEncodingCheckAlone(t *testing.T) {
	t.Parallel()

	raw := "tenant\x00a"

	if !utf8.ValidString(raw) {
		t.Fatal("this test's premise is wrong: NUL would have been caught by an encoding check")
	}

	if unchanged := strings.ToValidUTF8(raw, HeaderValueReplacement); unchanged != raw {
		t.Fatal("this test's premise is wrong: ToValidUTF8 already removes NUL")
	}

	if got := SanitizeHeaderValue([]byte(raw)); strings.IndexByte(got, 0) >= 0 {
		t.Fatalf("SanitizeHeaderValue(%q) = %q still carries a NUL", raw, got)
	}
}

// Sanitizing GROWS the value: each replaced byte becomes three. Stated here so
// a caller applying a byte budget knows to apply it after this, not before.
func TestSanitizeHeaderValue_MayGrowTheValue(t *testing.T) {
	t.Parallel()

	input := []byte{0x00, 0x00}

	if got := SanitizeHeaderValue(input); len(got) <= len(input) {
		t.Fatalf("SanitizeHeaderValue(%q) = %q (%d bytes), expected growth from %d",
			input, got, len(got), len(input))
	}
}
