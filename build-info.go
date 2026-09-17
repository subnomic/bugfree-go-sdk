package bugfree

import (
	"path"
	"runtime/debug"
	"strings"
	"sync"
)

// buildInfo is what the Go toolchain writes into every binary about its build:
// no source, but the module and, built inside a git checkout, the commit.
type buildInfo struct {
	// module is the main module's path; empty when unknown ("go run file.go").
	module    string
	revision  string
	time      string
	modified  bool
	goVersion string
}

// readBuildInfo reads the binary's build information once.
var readBuildInfo = sync.OnceValue(func() buildInfo {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return buildInfo{}
	}
	build := buildInfo{module: info.Main.Path, goVersion: info.GoVersion}
	if build.module == "command-line-arguments" {
		build.module = ""
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			build.revision = setting.Value
		case "vcs.time":
			build.time = setting.Value
		case "vcs.modified":
			build.modified = setting.Value == "true"
		}
	}
	return build
})

// shortRevision is the commit as people write it.
func (b buildInfo) shortRevision() string {
	if len(b.revision) > 12 {
		return b.revision[:12]
	}
	return b.revision
}

// release names the build when the options name none: module@commit, marked
// "-dirty" when the checkout had changes that were not committed.
func (b buildInfo) release() string {
	if b.revision == "" {
		return ""
	}
	name := b.shortRevision()
	if b.module != "" {
		name = b.module + "@" + name
	}
	if b.modified {
		name += "-dirty"
	}
	return name
}

// ownsFunction reports whether a function belongs to the main module.
func (b buildInfo) ownsFunction(function string) bool {
	if b.module == "" {
		return false
	}
	return strings.HasPrefix(function, b.module+".") || strings.HasPrefix(function, b.module+"/") ||
		strings.HasPrefix(function, "main.")
}

// repositoryPath turns a frame into the path of its file inside the repository,
// the same on every machine, so a person can open it in their own checkout:
// "internal/orders/orders.go".
//
// A -trimpath build reports "example.com/shop/internal/orders/orders.go", the
// module path first; a normal build reports an absolute path of the build machine,
// and the function's package tells which directory of the module it is in.
func (b buildInfo) repositoryPath(frame Frame) string {
	if b.module == "" || frame.File == "" {
		return ""
	}
	if relative, ok := strings.CutPrefix(frame.File, b.module+"/"); ok {
		return relative
	}
	name := path.Base(frame.File)
	pkg := packageOf(frame.Function)
	switch {
	case pkg == b.module || pkg == "main":
		// The module root, or a main package whose directory the name does not say:
		// the absolute path is cut after the module's last element when it appears.
		root := "/" + path.Base(b.module) + "/"
		if index := strings.LastIndex(frame.File, root); index >= 0 {
			return frame.File[index+len(root):]
		}
		return name
	case strings.HasPrefix(pkg, b.module+"/"):
		return strings.TrimPrefix(pkg, b.module+"/") + "/" + name
	}
	return ""
}
