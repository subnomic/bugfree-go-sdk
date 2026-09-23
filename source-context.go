package bugfree

import (
	"bufio"
	"io/fs"
	"os"
	"strings"
	"sync"
)

// sourceReader reads source files and caches their lines.
//
// One request can hold several frames of the same file; re-reading it for every
// frame would be pointless disk access.
type sourceReader struct {
	roots map[string]string
	fsys  fs.FS

	mu    sync.RWMutex
	cache map[string][]string
}

// newSourceReader builds the reader.
func newSourceReader(roots map[string]string, fsys fs.FS) *sourceReader {
	return &sourceReader{
		roots: roots,
		fsys:  fsys,
		cache: make(map[string][]string),
	}
}

// maxSourceLines bounds the memory use on very large files.
const maxSourceLines = 20_000

// addContext attaches the source around the failing line to every frame.
func (r *sourceReader) addContext(frames []Frame, around int) {
	if around <= 0 {
		return
	}
	for i := range frames {
		lines, ok := r.lines(frames[i].File, frames[i].Path)
		if !ok {
			continue
		}
		frames[i].Context = sliceContext(lines, frames[i].Line, around)
	}
}

// sliceContext extracts the window around the failing line.
//
// Line numbers are 1-based; the interface finds the failing line by comparing
// against frame.Line, so the real numbers are preserved.
func sliceContext(lines []string, line, around int) []ContextLine {
	if line <= 0 || line > len(lines) {
		return nil
	}

	start := max(line-around, 1)
	end := min(line+around, len(lines))

	context := make([]ContextLine, 0, end-start+1)
	for number := start; number <= end; number++ {
		context = append(context, ContextLine{
			Line:   number,
			Source: strings.TrimRight(lines[number-1], "\r"),
		})
	}
	return context
}

// lines returns the file's lines (cached). repoPath is the file's path inside the
// repository, when known.
func (r *sourceReader) lines(file, repoPath string) ([]string, bool) {
	if file == "" {
		return nil, false
	}

	r.mu.RLock()
	cached, ok := r.cache[file]
	r.mu.RUnlock()
	if ok {
		return cached, cached != nil
	}

	lines := r.read(file, repoPath)

	r.mu.Lock()
	r.cache[file] = lines
	r.mu.Unlock()

	return lines, lines != nil
}

// read tries to read the file from the possible locations.
func (r *sourceReader) read(file, repoPath string) []string {
	for _, candidate := range r.candidates(file, repoPath) {
		if lines := r.readFrom(candidate); lines != nil {
			return lines
		}
	}
	return nil
}

// candidates produces the locations to try for a frame path.
//
// The order matters: the explicit mapping, then the file itself (an absolute path
// works directly during development), then the relative path under the roots,
// and last the path inside the repository.
func (r *sourceReader) candidates(file, repoPath string) []string {
	candidates := make([]string, 0, 2*len(r.roots)+3)

	for prefix, root := range r.roots {
		if prefix != "" && strings.HasPrefix(file, prefix) {
			candidates = append(candidates, strings.TrimSuffix(root, "/")+"/"+strings.TrimPrefix(file, prefix))
		}
	}
	candidates = append(candidates, file)

	// Paths built with -trimpath are relative, so they are also tried directly
	// under the roots.
	for _, root := range r.roots {
		candidates = append(candidates, strings.TrimSuffix(root, "/")+"/"+strings.TrimPrefix(file, "/"))
	}

	// A -trimpath path starts with the module path ("example.com/shop/internal/x.go"),
	// which is no directory on disk. The repository path is: relative to the working
	// directory when the program runs from the repository root, as `go run` and most
	// containers do, or under a root.
	if repoPath != "" && repoPath != file {
		candidates = append(candidates, repoPath)
		for _, root := range r.roots {
			candidates = append(candidates, strings.TrimSuffix(root, "/")+"/"+repoPath)
		}
	}
	return candidates
}

// readFrom reads from a single location; with an embedded FS it tries that first.
func (r *sourceReader) readFrom(path string) []string {
	if r.fsys != nil {
		if lines := readFS(r.fsys, strings.TrimPrefix(path, "/")); lines != nil {
			return lines
		}
	}

	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	return scanLines(file)
}

// readFS reads from the embedded file system.
func readFS(fsys fs.FS, name string) []string {
	file, err := fsys.Open(name)
	if err != nil {
		return nil
	}
	defer file.Close()
	return scanLines(file)
}

// scanLines splits the stream into lines.
func scanLines(reader interface{ Read([]byte) (int, error) }) []string {
	scanner := bufio.NewScanner(reader)
	// Long lines (generated code) can exceed the default 64KB limit.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	lines := make([]string, 0, 256)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) >= maxSourceLines {
			break
		}
	}
	if scanner.Err() != nil && len(lines) == 0 {
		return nil
	}
	return lines
}
