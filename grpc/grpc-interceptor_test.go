package bugfreegrpc

import (
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	bugfree "github.com/subnomic/bugfree-go-sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// recordingTransport keeps the sent events in memory.
type recordingTransport struct {
	mu     sync.Mutex
	events []*bugfree.Event
}

func (t *recordingTransport) Send(event *bugfree.Event) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, event)
}

func (t *recordingTransport) Flush(time.Duration) bool { return true }
func (t *recordingTransport) Close()                   {}

func (t *recordingTransport) all() []*bugfree.Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*bugfree.Event(nil), t.events...)
}

func install(t *testing.T) *recordingTransport {
	t.Helper()
	transport := &recordingTransport{}
	if err := bugfree.Init(bugfree.Options{DSN: "http://key@localhost:3000/ingest", Transport: transport}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bugfree.Close)
	return transport
}

var quiet = log.New(io.Discard, "", 0)

func TestUnaryInterceptorRecordsPanicWithTheCallsScope(t *testing.T) {
	transport := install(t)
	interceptor := UnaryServerInterceptor(Options{Logger: quiet})

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "req-9"))
	info := &grpc.UnaryServerInfo{FullMethod: "/orders.v1.Orders/Get"}
	response, err := interceptor(ctx, nil, info, func(ctx context.Context, _ any) (any, error) {
		bugfree.ScopeFromContext(ctx).SetUser(bugfree.User{ID: "alice"})
		panic("nil order")
	})

	if response != nil || status.Code(err) != codes.Internal {
		t.Fatalf("response = %v, err = %v, expected codes.Internal", response, err)
	}
	events := transport.all()
	if len(events) != 1 {
		t.Fatalf("got %d events", len(events))
	}
	event := events[0]
	if event.Level != bugfree.LevelFatal || event.Tags["grpc.method"] != "/orders.v1.Orders/Get" || event.TraceID != "req-9" {
		t.Errorf("event = %s tags=%v trace=%q", event.Level, event.Tags, event.TraceID)
	}
	if event.User == nil || event.User.ID != "alice" {
		t.Errorf("user = %+v, expected the one set on the call's scope", event.User)
	}
}

func TestUnaryInterceptorCapturesServerFaultsOnly(t *testing.T) {
	transport := install(t)
	interceptor := UnaryServerInterceptor(Options{CaptureErrors: true})
	info := &grpc.UnaryServerInfo{FullMethod: "/orders.v1.Orders/Pay"}

	calls := []error{
		status.Error(codes.NotFound, "order not found"),
		status.Error(codes.InvalidArgument, "amount is negative"),
		status.Error(codes.Internal, "ledger is unreachable"),
		nil,
	}
	for _, returned := range calls {
		_, err := interceptor(context.Background(), nil, info, func(context.Context, any) (any, error) {
			return nil, returned
		})
		if !errors.Is(err, returned) && err != returned {
			t.Errorf("err = %v, the handler's error has to pass through", err)
		}
	}

	events := transport.all()
	if len(events) != 1 {
		t.Fatalf("got %d events, expected only the Internal one", len(events))
	}
	if !strings.Contains(events[0].Message, "ledger is unreachable") || events[0].Tags["grpc.code"] != "Internal" {
		t.Errorf("event = %q tags=%v", events[0].Message, events[0].Tags)
	}
	if events[0].Culprit != "/orders.v1.Orders/Pay" || len(events[0].Stacktrace) != 0 {
		t.Errorf("culprit = %q with %d frames, expected the method and no interceptor stack", events[0].Culprit, len(events[0].Stacktrace))
	}
}

// fakeStream is the smallest grpc.ServerStream.
type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *fakeStream) Context() context.Context { return s.ctx }

func TestStreamInterceptorScopesTheStreamAndRecordsPanic(t *testing.T) {
	transport := install(t)
	interceptor := StreamServerInterceptor(Options{Logger: quiet})
	info := &grpc.StreamServerInfo{FullMethod: "/feed.v1.Feed/Watch"}

	err := interceptor(nil, &fakeStream{ctx: context.Background()}, info, func(_ any, stream grpc.ServerStream) error {
		scope := bugfree.ScopeFromContext(stream.Context())
		if scope == nil {
			t.Error("the stream carries no scope")
		}
		scope.SetTag("tenant", "acme")
		panic("stream broke")
	})

	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, expected codes.Internal", err)
	}
	events := transport.all()
	if len(events) != 1 || events[0].Tags["tenant"] != "acme" {
		t.Fatalf("events = %+v, expected one with the stream scope's tag", events)
	}
}

func TestRepanicReRaises(t *testing.T) {
	install(t)
	interceptor := UnaryServerInterceptor(Options{Repanic: true})

	defer func() {
		if recover() == nil {
			t.Error("the panic was swallowed")
		}
	}()
	_, _ = interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x"}, func(context.Context, any) (any, error) {
		panic("again")
	})
}
