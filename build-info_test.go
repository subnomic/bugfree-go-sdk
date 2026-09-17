package bugfree

import (
	"errors"
	"testing"
)

func TestBuildInfoOwnsOnlyTheMainModule(t *testing.T) {
	build := buildInfo{module: "example.com/shop"}
	cases := map[string]bool{
		"example.com/shop/internal/orders.(*Order).Charge": true,
		"example.com/shop.run":                             true,
		"main.handler":                                     true,
		"example.com/shopping/cart.Add":                    false,
		"github.com/gin-gonic/gin.(*Context).Next":         false,
		"github.com/subnomic/bugfree-go-sdk.Middleware":    false,
		"net/http.HandlerFunc.ServeHTTP":                   false,
		"runtime.gopanic":                                  false,
	}
	for function, want := range cases {
		if got := build.ownsFunction(function); got != want {
			t.Errorf("ownsFunction(%q) = %v, expected %v", function, got, want)
		}
	}
	if (buildInfo{}).ownsFunction("main.handler") {
		t.Error("without a module nothing can be claimed")
	}
}

func TestRepositoryPathFromTrimpathAndAbsolutePaths(t *testing.T) {
	build := buildInfo{module: "example.com/shop"}
	cases := []struct {
		frame Frame
		want  string
	}{
		// -trimpath: the module path leads the file.
		{Frame{Function: "example.com/shop/internal/orders.(*Order).Charge", File: "example.com/shop/internal/orders/orders.go"}, "internal/orders/orders.go"},
		{Frame{Function: "main.handler", File: "example.com/shop/main.go"}, "main.go"},
		// A normal build: an absolute path of the build machine.
		{Frame{Function: "example.com/shop/internal/orders.(*Order).Charge", File: "/home/ci/work/src/internal/orders/orders.go"}, "internal/orders/orders.go"},
		{Frame{Function: "main.handler", File: "/Users/jane/code/shop/cmd/api/main.go"}, "cmd/api/main.go"},
		{Frame{Function: "main.handler", File: "/build/main.go"}, "main.go"},
		{Frame{Function: "github.com/gin-gonic/gin.(*Context).Next", File: "github.com/gin-gonic/gin@v1.12.0/context.go"}, ""},
	}
	for _, tc := range cases {
		if got := build.repositoryPath(tc.frame); got != tc.want {
			t.Errorf("repositoryPath(%s) = %q, expected %q", tc.frame.File, got, tc.want)
		}
	}
}

func TestReleaseNamesTheCommit(t *testing.T) {
	cases := map[string]buildInfo{
		"example.com/shop@64d287a6e451":       {module: "example.com/shop", revision: "64d287a6e4515ac60e243468a8f43449ab5cafe7"},
		"example.com/shop@64d287a6e451-dirty": {module: "example.com/shop", revision: "64d287a6e4515ac60e243468a8f43449ab5cafe7", modified: true},
		"":                                    {module: "example.com/shop"},
	}
	for want, build := range cases {
		if got := build.release(); got != want {
			t.Errorf("release() = %q, expected %q", got, want)
		}
	}
}

// A binary without build information still tells trimpath dependencies and the
// standard library apart from application code.
func TestInAppByPathReadsTrimpathPaths(t *testing.T) {
	cases := map[string]bool{
		"github.com/gin-gonic/gin@v1.12.0/context.go":     false,
		"net/http/server.go":                              false,
		"example.com/shop/internal/orders/orders.go":      true,
		"/home/me/go/pkg/mod/github.com/x/y@v1.0.0/y.go":  false,
		"/Users/jane/code/shop/internal/orders/orders.go": true,
	}
	for file, want := range cases {
		if got := inAppByPath(Frame{File: file, Function: "x.y"}); got != want {
			t.Errorf("inAppByPath(%q) = %v, expected %v", file, got, want)
		}
	}
}

func TestEventsCarryTheRepositoryPathOfApplicationFrames(t *testing.T) {
	client, transport := newTestClient(t, nil)
	client.CaptureException(errors.New("where"))

	frame := transport.last().Stacktrace[0]
	if !frame.InApp || frame.Path != "build-info_test.go" {
		t.Errorf("frame = %+v, expected the test file as an application frame", frame)
	}
}
