package wire

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func TestReadPipelinedFramesAcrossBlock(t *testing.T) {
	first := strings.Repeat("a", 4093)
	last := strings.Repeat("b", 8190)
	reader := Reader(strings.NewReader(`"` + first + "\"\r\n\"short\"\n\"second\"\n\"third\"\n\"" + last + "\""))
	for _, want := range []string{first, "short", "second", "third", last} {
		var got string
		if err := Read(reader, &got); err != nil || got != want {
			t.Fatalf("frame of length %d decoded as %d bytes: %v", len(want), len(got), err)
		}
	}
	var got string
	if err := Read(reader, &got); !errors.Is(err, io.EOF) || reader.Err() != nil {
		t.Fatalf("end of stream: %v (scanner: %v)", err, reader.Err())
	}
}

func TestReadFrameSizeLimit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		size    int
		suffix  string
		tooLong bool
	}{
		{"max_with_lf", MaxFrame, "\n", false},
		{"max_with_crlf", MaxFrame, "\r\n", false},
		{"over_max_with_lf", MaxFrame + 1, "\n", true},
		{"below_max_without_lf", MaxFrame - 1, "", false},
		{"max_without_lf", MaxFrame, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			width := tc.size - 2 - len(tc.suffix)
			raw := `"` + strings.Repeat("x", width) + `"` + tc.suffix
			reader := Reader(strings.NewReader(raw))
			var got string
			err := Read(reader, &got)
			if tc.tooLong {
				if !errors.Is(err, bufio.ErrTooLong) || !errors.Is(reader.Err(), bufio.ErrTooLong) {
					t.Fatalf("overlong frame: %v (scanner: %v)", err, reader.Err())
				}
			} else if err != nil || len(got) != width {
				t.Fatalf("decoded %d of %d bytes: %v", len(got), width, err)
			}
		})
	}
}

func TestReadPreservesTransportErrorAfterLongFrame(t *testing.T) {
	closed := errors.New("closed transport")
	want := strings.Repeat("a", 8190)
	in := io.MultiReader(strings.NewReader(`"`+want+`"`), iotest.ErrReader(closed))
	reader := Reader(in)
	var got string
	if err := Read(reader, &got); err != nil || got != want {
		t.Fatalf("final frame decoded as %d bytes: %v", len(got), err)
	}
	if err := Read(reader, &got); !errors.Is(err, closed) || !errors.Is(reader.Err(), closed) {
		t.Fatalf("end of stream: %v (scanner: %v)", err, reader.Err())
	}
}

func TestReadShrinksAfterTwoShortFrames(t *testing.T) {
	large := strings.Repeat("x", 8192)
	largeFrame := `"` + large + "\"\n"
	input := largeFrame + "\"short\"\n" + largeFrame + "\"one\"\n\"two\"\n\"tail\"\n"
	reader := Reader(strings.NewReader(input))
	grown := reader.scanner
	for _, want := range []string{large, "short", large, "one"} {
		var got string
		if err := Read(reader, &got); err != nil || got != want || reader.scanner != grown {
			t.Fatalf("expected the same scanner and a frame of length %d: got %d bytes, %v", len(want), len(got), err)
		}
	}
	var got string
	if err := Read(reader, &got); err != nil || got != "two" || reader.scanner == grown {
		t.Fatalf("second short frame did not shrink the reader: %q, %v", got, err)
	}
	if err := Read(reader, &got); err != nil || got != "tail" {
		t.Fatalf("lost read-ahead after shrinking: %q, %v", got, err)
	}
}
