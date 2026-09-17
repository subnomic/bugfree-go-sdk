package bugfree

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
)

func TestSliceContextCentersFailingLine(t *testing.T) {
	lines := []string{"one", "two", "three", "four", "five", "six", "seven"}

	context := sliceContext(lines, 4, 2)
	if len(context) != 5 {
		t.Fatalf("context length = %d, expected 5", len(context))
	}
	if context[0].Line != 2 || context[0].Source != "two" {
		t.Errorf("first line = %d/%q, expected 2/two", context[0].Line, context[0].Source)
	}
	if context[2].Line != 4 || context[2].Source != "four" {
		t.Errorf("middle line = %d/%q, expected the error line 4/four", context[2].Line, context[2].Source)
	}
	if context[4].Line != 6 {
		t.Errorf("last line = %d, expected 6", context[4].Line)
	}
}

// The start and end of a file must not overflow the window.
func TestSliceContextStaysInBounds(t *testing.T) {
	lines := []string{"a", "b", "c"}

	if context := sliceContext(lines, 1, 5); context[0].Line != 1 || len(context) != 3 {
		t.Errorf("first line window = %d lines starting at %d, expected 3 starting at 1",
			len(context), context[0].Line)
	}
	if context := sliceContext(lines, 3, 5); len(context) != 3 {
		t.Errorf("last line window = %d lines, expected 3", len(context))
	}
	if context := sliceContext(lines, 0, 2); context != nil {
		t.Error("expected nil for line 0")
	}
	if context := sliceContext(lines, 99, 2); context != nil {
		t.Error("expected nil for a line beyond the file")
	}
}

func TestSourceReaderReadsFromDisk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "handler.go")
	source := "package main\n\nfunc Charge() {\n\tpanic(\"boom\")\n}\n"
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}

	reader := newSourceReader(nil, nil)
	frames := []Frame{{File: path, Line: 4}}
	reader.addContext(frames, 2)

	if len(frames[0].Context) == 0 {
		t.Fatal("no source context was attached")
	}
	found := false
	for _, line := range frames[0].Context {
		if line.Line == 4 && line.Source == "\tpanic(\"boom\")" {
			found = true
		}
	}
	if !found {
		t.Errorf("the error line was not read: %+v", frames[0].Context)
	}
}

// In a binary built with -trimpath the frame path is relative; the root mapping resolves it.
func TestSourceReaderUsesRootMapping(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "internal", "service"), 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "internal", "service", "issue.go")
	if err := os.WriteFile(path, []byte("line1\nline2\nline3\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	reader := newSourceReader(map[string]string{
		"github.com/bugfree/backend/": dir + "/",
	}, nil)

	frames := []Frame{{File: "github.com/bugfree/backend/internal/service/issue.go", Line: 2}}
	reader.addContext(frames, 1)

	if len(frames[0].Context) != 3 {
		t.Fatalf("context length = %d, expected 3", len(frames[0].Context))
	}
	if frames[0].Context[1].Source != "line2" {
		t.Errorf("error line = %q, expected line2", frames[0].Context[1].Source)
	}
}

// An embedded FS is supported for containers that ship no source.
func TestSourceReaderReadsFromEmbeddedFS(t *testing.T) {
	fsys := fstest.MapFS{
		"internal/handler/pay.go": &fstest.MapFile{Data: []byte("a\nb\nc\nd\n")},
	}
	reader := newSourceReader(nil, fsys)

	frames := []Frame{{File: "internal/handler/pay.go", Line: 3}}
	reader.addContext(frames, 1)

	if len(frames[0].Context) != 3 {
		t.Fatalf("context length = %d, expected 3", len(frames[0].Context))
	}
	if frames[0].Context[1].Source != "c" {
		t.Errorf("error line = %q, expected c", frames[0].Context[1].Source)
	}
}

func TestSourceReaderSkipsMissingFile(t *testing.T) {
	reader := newSourceReader(nil, nil)
	frames := []Frame{{File: "/nowhere/missing.go", Line: 10}}
	reader.addContext(frames, 3)

	if frames[0].Context != nil {
		t.Error("context was attached for a missing file")
	}
}

// A negative value turns source reading off completely (privacy/performance).
func TestSourceReaderReadsNothingOnNegativeValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.go")
	if err := os.WriteFile(path, []byte("a\nb\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	reader := newSourceReader(nil, nil)
	frames := []Frame{{File: path, Line: 1}}
	reader.addContext(frames, -1)

	if frames[0].Context != nil {
		t.Error("source was read even though context lines were disabled")
	}
}

// The same file appearing in several frames must not be read again.
func TestSourceReaderCaches(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cached.go")
	if err := os.WriteFile(path, []byte("a\nb\nc\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	reader := newSourceReader(nil, nil)
	frames := []Frame{{File: path, Line: 1}, {File: path, Line: 3}}
	reader.addContext(frames, 1)

	if len(reader.cache) != 1 {
		t.Errorf("cache holds %d entries, expected 1", len(reader.cache))
	}
	if len(frames[1].Context) == 0 {
		t.Error("the second frame got no context")
	}
}

// A -trimpath binary names its files by module path, which is no directory on
// disk; the repository path finds them from the repository root without any roots.
func TestSourceReaderFindsTrimmedPathFromRepositoryRoot(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "internal", "pay"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "internal", "pay", "pay.go"), []byte("a\nb\nc\nd\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)

	reader := newSourceReader(nil, nil)
	frames := []Frame{{File: "example.com/shop/internal/pay/pay.go", Path: "internal/pay/pay.go", Line: 3}}
	reader.addContext(frames, 1)

	if len(frames[0].Context) != 3 || frames[0].Context[1].Source != "c" {
		t.Errorf("context = %+v, expected b, c, d around line 3", frames[0].Context)
	}
}
