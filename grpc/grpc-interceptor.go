// Package bugfreegrpc joins the bugfree SDK with gRPC servers.
//
// It is a separate module so the core SDK has no gRPC dependency; projects that
// do not use gRPC never pull it in.
//
// Usage:
//
//	server := grpc.NewServer(
//	    grpc.ChainUnaryInterceptor(bugfreegrpc.UnaryServerInterceptor(bugfreegrpc.Options{})),
//	    grpc.ChainStreamInterceptor(bugfreegrpc.StreamServerInterceptor(bugfreegrpc.Options{})),
//	)
package bugfreegrpc

import (
	"context"
	"errors"
	"io"
	"log"
	"strings"

	bugfree "github.com/subnomic/bugfree-go-sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Options is the interceptors' behaviour.
type Options struct {
	// Repanic re-raises the panic after it is recorded, for a recovery layer of
	// your own further out. Otherwise the call ends with codes.Internal.
	Repanic bool

	// Logger receives the panic and its stack when the panic is not re-raised;
	// the standard logger when nil.
	Logger *log.Logger

	// CaptureErrors also records the errors handlers return with a code that means
	// a server fault: Unknown, Internal and DataLoss (or the codes in ErrorCodes).
	CaptureErrors bool

	// ErrorCodes replaces the codes CaptureErrors records.
	ErrorCodes []codes.Code

	// UserResolver returns the user of the call, from its context (the
	// authentication interceptor's values, say).
	UserResolver func(ctx context.Context) bugfree.User
}

// defaultErrorCodes are the codes that mean the server failed, not the caller.
var defaultErrorCodes = []codes.Code{codes.Unknown, codes.Internal, codes.DataLoss}

// UnaryServerInterceptor gives every call a scope of its own and records its
// panic, and with CaptureErrors its server-fault errors.
//
// Inside a handler, bugfree.ScopeFromContext(ctx) is the call's scope.
func UnaryServerInterceptor(options Options) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
		ctx = bugfree.ContextWithScope(ctx)
		ctx, transaction := bugfree.StartTransaction(ctx, info.FullMethod,
			bugfree.WithOp("grpc.server"), bugfree.ContinueTrace(traceparentOf(ctx)))

		defer func() {
			recovered := recover()
			if recovered == nil {
				finishTransaction(ctx, transaction, err)
				return
			}
			transaction.SetStatus("internal_error")
			transaction.Finish()
			bugfree.Current().RecordSession(bugfree.SessionCrashed)
			bugfree.Current().CaptureRecoveredContext(ctx, recovered, 2, modifiers(ctx, info.FullMethod, options)...)
			if options.Repanic {
				panic(recovered)
			}
			bugfree.LogPanic(options.Logger, recovered)
			response, err = nil, status.Error(codes.Internal, "internal error")
		}()

		response, err = handler(ctx, request)
		recordSession(ctx, err)
		if options.CaptureErrors {
			captureError(ctx, info.FullMethod, err, options)
		}
		return response, err
	}
}

// StreamServerInterceptor is UnaryServerInterceptor for streaming calls. The
// handler's stream carries the call's scope in its Context.
func StreamServerInterceptor(options Options) grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		ctx := bugfree.ContextWithScope(stream.Context())
		ctx, transaction := bugfree.StartTransaction(ctx, info.FullMethod,
			bugfree.WithOp("grpc.server"), bugfree.ContinueTrace(traceparentOf(ctx)))
		wrapped := &scopedStream{ServerStream: stream, ctx: ctx}

		defer func() {
			recovered := recover()
			if recovered == nil {
				finishTransaction(ctx, transaction, err)
				return
			}
			transaction.SetStatus("internal_error")
			transaction.Finish()
			bugfree.Current().RecordSession(bugfree.SessionCrashed)
			bugfree.Current().CaptureRecoveredContext(ctx, recovered, 2, modifiers(ctx, info.FullMethod, options)...)
			if options.Repanic {
				panic(recovered)
			}
			bugfree.LogPanic(options.Logger, recovered)
			err = status.Error(codes.Internal, "internal error")
		}()

		err = handler(server, wrapped)
		recordSession(ctx, err)
		if options.CaptureErrors {
			captureError(ctx, info.FullMethod, err, options)
		}
		return err
	}
}

// recordSession counts a finished call for release health: a server-fault code
// counts as errored, a call the client left does not.
func recordSession(ctx context.Context, err error) {
	if !abandoned(ctx, err) && containsCode(defaultErrorCodes, status.Code(err)) {
		bugfree.Current().RecordSession(bugfree.SessionErrored)
		return
	}
	bugfree.Current().RecordSession(bugfree.SessionOK)
}

// scopedStream hands the call's scope to the stream handler.
type scopedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *scopedStream) Context() context.Context { return s.ctx }

// captureError records a returned error whose code means a server fault.
func captureError(ctx context.Context, method string, err error, options Options) {
	// gRPC calls every error without a status Unknown, a dropped connection among
	// them; recorded, each client that goes away would be an event.
	if err == nil || abandoned(ctx, err) {
		return
	}
	code := status.Code(err)
	watched := options.ErrorCodes
	if len(watched) == 0 {
		watched = defaultErrorCodes
	}
	if !containsCode(watched, code) {
		return
	}

	recorded := bugfree.StackOf(err)
	eventModifiers := append(modifiers(ctx, method, options),
		bugfree.WithTag("grpc.code", code.String()),
		func(event *bugfree.Event) {
			if recorded != nil {
				return
			}
			// The stack here is the interceptor's, the same for every call; without it
			// the server groups by the error's type, the method and the message.
			event.Stacktrace = nil
			event.Culprit = method
		},
	)
	bugfree.Current().CaptureExceptionContext(ctx, err, eventModifiers...)
}

// modifiers builds what every event of a call carries.
func modifiers(ctx context.Context, method string, options Options) []bugfree.EventModifier {
	eventModifiers := []bugfree.EventModifier{
		bugfree.WithTag("handler", "grpc"),
		bugfree.WithTag("grpc.method", method),
	}
	if requestID := requestIDOf(ctx); requestID != "" {
		eventModifiers = append(eventModifiers, bugfree.WithTraceID(requestID))
	}
	if options.UserResolver != nil {
		eventModifiers = append(eventModifiers, bugfree.WithUser(options.UserResolver(ctx)))
	}
	return eventModifiers
}

// abandoned reports whether a call ended because its client went away or gave up,
// rather than because the server failed: its context is over, or the error is
// an end of stream, a cancellation or a deadline.
func abandoned(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx.Err() != nil {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return true
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// finishTransaction ends a call's transaction with the status its error names.
func finishTransaction(ctx context.Context, transaction *bugfree.Span, err error) {
	if abandoned(ctx, err) {
		transaction.SetStatus("cancelled")
	} else if code := status.Code(err); code != codes.OK {
		transaction.SetStatus(strings.ToLower(code.String()))
	}
	transaction.Finish()
}

// traceparentOf reads the traceparent metadata of the incoming call.
func traceparentOf(ctx context.Context) string {
	incoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if values := incoming.Get("traceparent"); len(values) > 0 {
		return values[0]
	}
	return ""
}

// requestIDOf reads the x-request-id metadata of the incoming call.
func requestIDOf(ctx context.Context) string {
	incoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if values := incoming.Get("x-request-id"); len(values) > 0 {
		return values[0]
	}
	return ""
}

func containsCode(list []codes.Code, code codes.Code) bool {
	for _, candidate := range list {
		if candidate == code {
			return true
		}
	}
	return false
}
