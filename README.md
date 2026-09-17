# bugfree-go

Go SDK for bugfree.

```sh
go get github.com/subnomic/bugfree-go-sdk@latest
go get github.com/subnomic/bugfree-go-sdk/gin@latest   # only if you use gin
go get github.com/subnomic/bugfree-go-sdk/grpc@latest  # only if you use gRPC
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
    // A malformed DSN must not take the application down: report it and go on
    // without monitoring.
    log.Printf("bugfree: monitoring is off: %v", err)
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

// Every capture returns the event id ("" when nothing was sent)
id := bugfree.CaptureExceptionContext(ctx, err)

// Panics — recorded and re-thrown
defer bugfree.Recover()

// Panics — recorded and swallowed
defer bugfree.RecoverAndContinue()
```

Errors wrapped with `%w` or `errors.Join` are listed on the issue under
*Caused by*.

A returned error usually reaches the reporter far from where it happened.
Record the stack where it is created, and every capture uses that stack instead
of its own:

```go
if err != nil {
    return bugfree.WithStack(err)          // or bugfree.Errorf("load %s: %w", id, err)
}
```

The same error sent again within `DedupeWindow` (1s) is held back; the next copy
sent carries `repeats_dropped`. `Client.Stats()` counts every event that was not
sent, by reason.

## Context

The package-level context is global: every event of the process carries it.

```go
bugfree.SetTag("region", "eu-central")
bugfree.AddBreadcrumb(bugfree.Breadcrumb{Category: "boot", Message: "config loaded"})
```

Anything that belongs to one request, above all the user, goes on a scope of
its own. The middlewares below create one per request; it inherits the global
tags and user, but not the global breadcrumbs (in a server those are every other
request's), and what is set on it stays with that request:

```go
scope := bugfree.ScopeFromContext(r.Context())
scope.SetUser(bugfree.User{ID: "u-1"})
scope.AddBreadcrumb(bugfree.Breadcrumb{Category: "sql", Message: "SELECT 1"})

bugfree.CaptureExceptionContext(r.Context(), err)

// Outside a middleware (a job, a consumer):
ctx = bugfree.ContextWithScope(ctx)
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
    // Also record errors attached with c.Error on a 5xx response:
    CaptureErrors: true,
}))
router.Use(bugfreegin.Breadcrumbs()) // after Recovery: records on the request's scope

// Inside a handler: bugfreegin.Scope(c) is the request's scope.

// gRPC
server := grpc.NewServer(
    grpc.ChainUnaryInterceptor(bugfreegrpc.UnaryServerInterceptor(bugfreegrpc.Options{CaptureErrors: true})),
    grpc.ChainStreamInterceptor(bugfreegrpc.StreamServerInterceptor(bugfreegrpc.Options{})),
)
```

A swallowed panic is always logged with its stack, so it is not lost where the
DSN is empty. `http.ErrAbortHandler` is re-raised, never recorded. Request
addresses are sent without their query string; gin's `SendQueryString` keeps it.
gin also takes `RequestIDResolver` and `OmitClientIP`.

## Tracing

```go
bugfree.Init(bugfree.Options{DSN: dsn, TracesSampleRate: 0.2})

// Middlewares time every request (continuing an incoming traceparent);
// BreadcrumbTransport and WrapDriver add outgoing calls and queries as spans.
ctx, span := bugfree.StartSpan(r.Context(), "payment.authorize", provider)
defer span.Finish()

// Outside a request:
ctx, transaction := bugfree.StartTransaction(ctx, "nightly import", bugfree.WithOp("task"))
defer transaction.Finish()
```

## Profiling

```go
bugfree.Init(bugfree.Options{
    DSN:               dsn,
    ProfilingInterval: 5 * time.Minute,  // a CPU and a memory profile every five minutes
    ProfileDuration:   15 * time.Second, // length of each CPU profile
})
```

## Cron monitors

```go
// Checks in when the job starts and ends; the config creates or updates the monitor.
err := bugfree.WithMonitor("nightly-backup", &bugfree.MonitorConfig{
    Schedule:   "0 3 * * *",
    Timezone:   "Europe/Istanbul",
    MaxRuntime: 45 * time.Minute,
}, backup)

// Or by hand: pass the returned id with the end of the run.
id := bugfree.CaptureCheckIn(bugfree.CheckIn{MonitorSlug: "sync", Status: bugfree.CheckInInProgress})
bugfree.CaptureCheckIn(bugfree.CheckIn{ID: id, MonitorSlug: "sync", Status: bugfree.CheckInOK})
```

A run that fails, overruns `MaxRuntime` or does not start within `CheckInMargin`
opens the monitor's issue; the next successful run resolves it. Check-ins are
sent at once (at most five seconds), since a job often exits right after.

## User feedback

```go
// Forward what a user wrote; EventID is the reference a capture call returned.
err := bugfree.CaptureFeedback(bugfree.Feedback{EventID: reference, Email: email, Message: message})
```

## Goroutines, logs, requests and SQL

```go
// A goroutine that records its panic instead of ending the program
bugfree.Go(ctx, func(ctx context.Context) { resize(ctx, upload) })

// slog: errors become events, lower levels breadcrumbs
slog.SetDefault(slog.New(bugfree.NewLogHandler(slog.NewJSONHandler(os.Stdout, nil), bugfree.LogHandlerOptions{})))

// Outgoing requests as breadcrumbs (address without its query string)
client := &http.Client{Transport: bugfree.BreadcrumbTransport(nil)}

// SQL statements as breadcrumbs (never their arguments)
sql.Register("pgx+bugfree", bugfree.WrapDriver(stdlib.GetDefaultDriver()))
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
| `SampleRate` | Fraction of events to send (default 1). `0` also means 1: to send nothing, leave the DSN empty. |
| `ProfilingInterval` | How often a CPU and a heap profile are taken (default 0: off). |
| `TracesSampleRate` | Share of traces timed, decided from the trace id (default 0: off). An incoming `traceparent`'s sampled flag is ignored. |
| `TrustIncomingSampling` | Follow the incoming sampled flag instead; only when every caller is your own. |
| `TrackSessions` | Count every request the middlewares handle as a session, for release health (crash-free rate). Reported once a minute. |
| `DedupeWindow` | How long an identical error is held back (default 1s; negative sends every copy). |
| `InAppPrefixes` | Which frames count as your code. Defaults to "not stdlib, not a dependency". |
| `BeforeSend` | Last chance to scrub or drop an event. |
| `MaxBreadcrumbs` | Ring buffer size (default 30). |
| `ShutdownTimeout` | How long `Close` waits for queued events before dropping them (default 5s). |
| `Transport` | Replace the HTTP transport (used by the SDK's own tests). |
| `Debug` | Log what the SDK is doing to stderr. |

A `429` answer pauses sending for as long as its `Retry-After` asks (a minute
when it names no time); events captured during the pause are dropped.

## Releasing

The SDK is published to `github.com/subnomic/bugfree-go-sdk`, with this
directory as that repository's root, by the bugfree release: one release on the
bugfree repository's Releases page with the tag `v0.6.0` publishes the server and
both SDKs at that version.

The release workflow checks that `Version` in `client.go` and the core module
version `gin/go.mod` and `grpc/go.mod` require all match the tag, runs the tests,
pushes this directory to that repository as one commit and tags it there as
`v0.6.0`, `gin/v0.6.0` and `grpc/v0.6.0`: the gin middleware and the gRPC
interceptors are nested modules with tags of their own. Their `replace` lines only
apply inside this repository.

## License

MIT, see [`LICENSE`](LICENSE).
