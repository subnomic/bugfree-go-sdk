# bugfree-go

Go SDK for bugfree.

```sh
go get github.com/subnomic/bugfree-go-sdk@latest
go get github.com/subnomic/bugfree-go-sdk/gin@latest   # only if you use gin
```

```go
import (
    bugfree "github.com/subnomic/bugfree-go-sdk"
    bugfreegin "github.com/subnomic/bugfree-go-sdk/gin"
)
```

## Setup

```go
err := bugfree.Init(bugfree.Options{
    DSN:                os.Getenv("BUGFREE_DSN"), // http://<key>@host:3000/ingest
    Environment:        "production",
    Release:            "payment-api@v3.14.2",
    SourceContextLines: 5,
})
if err != nil {
    log.Fatal(err)
}
defer bugfree.Close() // sends what is queued, waiting at most ShutdownTimeout (5s)
```

An empty DSN disables the SDK: every call below becomes a no-op.

## Capturing

```go
// Errors
bugfree.CaptureException(err,
    bugfree.WithTag("tenant", tenantID),
    bugfree.WithUser(bugfree.User{ID: userID}),
    bugfree.WithExtra("amount", amount),
)

// Messages
bugfree.CaptureMessage(bugfree.LevelWarning, "payment retried")

// Panics — recorded and re-thrown
defer bugfree.Recover()

// Panics — recorded and swallowed
defer bugfree.RecoverAndContinue()
```

## Context

```go
bugfree.SetUser(bugfree.User{ID: "u-1"})
bugfree.SetTag("region", "eu-central")
bugfree.AddBreadcrumb(bugfree.Breadcrumb{Category: "sql", Message: "SELECT 1"})
```

## Frameworks

```go
// net/http
http.ListenAndServe(":8080", bugfree.Middleware(mux))

// gin
router.Use(bugfreegin.Recovery(bugfreegin.Options{
    // Keep your own error response shape:
    OnPanic: func(c *gin.Context, _ any) {
        c.AbortWithStatusJSON(500, gin.H{"code": "internal_error"})
    },
}))
router.Use(bugfreegin.Breadcrumbs())
```

## Source code context

Frames carry the lines around the failure. In a `-trimpath` build the frame
paths are module-relative, so point the SDK at the sources:

```go
SourceRoots: map[string]string{"github.com/acme/api/": "/app/src/"},
// or embed them instead of shipping files:
SourceFS: sources, // //go:embed internal cmd
```

Set `SourceContextLines: -1` to never read source files.

## Options worth knowing

| Option | Meaning |
|---|---|
| `SampleRate` | Fraction of events to send (default 1). |
| `InAppPrefixes` | Which frames count as your code. Defaults to "not stdlib, not a dependency". |
| `BeforeSend` | Last chance to scrub or drop an event. |
| `MaxBreadcrumbs` | Ring buffer size (default 30). |
| `ShutdownTimeout` | How long `Close` waits for queued events before dropping them (default 5s). |
| `Transport` | Replace the HTTP transport (used by the SDK's own tests). |
| `Debug` | Log what the SDK is doing to stderr. |

## Releasing

The SDK is published to `github.com/subnomic/bugfree-go-sdk`, with this
directory as that repository's root. Raise the versions on `main`, then publish a
release on the bugfree repository's Releases page with the new tag `sdk-go-v0.2.0`.

The release workflow checks that `Version` in `client.go` and the core module
version `gin/go.mod` requires both match the tag, runs the tests, pushes this
directory to that repository as one commit and tags it there as `v0.2.0` and
`gin/v0.2.0`: the gin middleware is a nested module with its own tag.
`gin/go.mod`'s `replace` line only applies inside this repository.

## License

MIT, see [`LICENSE`](LICENSE).
