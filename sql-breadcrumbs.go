package bugfree

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"
)

// WrapDriver records every query run through a database/sql driver as a
// breadcrumb.
//
// Usage, with any driver:
//
//	sql.Register("pgx+bugfree", bugfree.WrapDriver(stdlib.GetDefaultDriver()))
//	db, err := sql.Open("pgx+bugfree", dsn)
//
// or, for a connector:
//
//	db := sql.OpenDB(bugfree.WrapConnector(connector))
//
// A query run with a context carrying a scope (QueryContext, ExecContext) records
// on that scope, the global one otherwise. Only the statement text is recorded,
// never the arguments bound to it.
func WrapDriver(inner driver.Driver) driver.Driver {
	return &sqlDriver{inner: inner}
}

// WrapConnector is WrapDriver for sql.OpenDB.
func WrapConnector(inner driver.Connector) driver.Connector {
	return &sqlConnector{inner: inner, driver: &sqlDriver{inner: inner.Driver()}}
}

// maxQueryLength bounds the statement text a breadcrumb carries.
const maxQueryLength = 500

// recordQuery adds the breadcrumb of one statement, and its span inside a
// transaction.
func recordQuery(ctx context.Context, query string, started time.Time, err error) {
	// ErrSkip is database/sql asking for another path, not a failed query.
	if errors.Is(err, driver.ErrSkip) {
		return
	}

	query = strings.Join(strings.Fields(query), " ")
	if len(query) > maxQueryLength {
		query = query[:maxQueryLength] + "…"
	}

	if _, span := StartSpan(ctx, "db.query", query); span != nil {
		// The statement already ran: the span starts when it did.
		span.Start = started
		if err != nil {
			span.SetStatus("internal_error")
		}
		span.Finish()
	}

	crumb := Breadcrumb{Category: "sql", Message: query, Level: "info"}
	if err != nil {
		crumb.Data = fmt.Sprintf("failed · %v", err)
		crumb.Level = "error"
	} else {
		crumb.Data = time.Since(started).Round(time.Microsecond).String()
	}

	scope := ScopeFromContext(ctx)
	if scope == nil {
		scope = Current().Scope()
	}
	scope.AddBreadcrumb(crumb)
}

type sqlDriver struct {
	inner driver.Driver
}

func (d *sqlDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &sqlConn{inner: conn}, nil
}

// OpenConnector keeps a driver's own connector when it has one.
func (d *sqlDriver) OpenConnector(name string) (driver.Connector, error) {
	if withContext, ok := d.inner.(driver.DriverContext); ok {
		connector, err := withContext.OpenConnector(name)
		if err != nil {
			return nil, err
		}
		return &sqlConnector{inner: connector, driver: d}, nil
	}
	return &sqlConnector{inner: dsnConnector{name: name, driver: d.inner}, driver: d}, nil
}

// dsnConnector opens connections of a driver that has no connector of its own.
type dsnConnector struct {
	name   string
	driver driver.Driver
}

func (c dsnConnector) Connect(context.Context) (driver.Conn, error) { return c.driver.Open(c.name) }
func (c dsnConnector) Driver() driver.Driver                        { return c.driver }

type sqlConnector struct {
	inner  driver.Connector
	driver *sqlDriver
}

func (c *sqlConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &sqlConn{inner: conn}, nil
}

func (c *sqlConnector) Driver() driver.Driver { return c.driver }

// sqlConn wraps a connection. Every optional interface database/sql looks for is
// implemented; where the driver lacks one, driver.ErrSkip sends database/sql down
// the path it would have taken without the wrapper.
type sqlConn struct {
	inner driver.Conn
}

func (c *sqlConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.inner.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &sqlStmt{inner: stmt, query: query}, nil
}

func (c *sqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	var stmt driver.Stmt
	var err error
	if preparer, ok := c.inner.(driver.ConnPrepareContext); ok {
		stmt, err = preparer.PrepareContext(ctx, query)
	} else {
		stmt, err = c.inner.Prepare(query)
	}
	if err != nil {
		return nil, err
	}
	return &sqlStmt{inner: stmt, query: query}, nil
}

func (c *sqlConn) Close() error { return c.inner.Close() }

// Begin is required by driver.Conn; database/sql uses BeginTx.
func (c *sqlConn) Begin() (driver.Tx, error) { return c.inner.Begin() }

func (c *sqlConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if beginner, ok := c.inner.(driver.ConnBeginTx); ok {
		return beginner.BeginTx(ctx, options)
	}
	// What database/sql itself refuses for a driver without BeginTx.
	if options.Isolation != driver.IsolationLevel(0) {
		return nil, errors.New("sql: driver does not support non-default isolation level")
	}
	if options.ReadOnly {
		return nil, errors.New("sql: driver does not support read-only transactions")
	}
	return c.inner.Begin()
}

func (c *sqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := c.inner.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	started := time.Now()
	result, err := execer.ExecContext(ctx, query, args)
	recordQuery(ctx, query, started, err)
	return result, err
}

func (c *sqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := c.inner.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	started := time.Now()
	rows, err := queryer.QueryContext(ctx, query, args)
	recordQuery(ctx, query, started, err)
	return rows, err
}

func (c *sqlConn) Ping(ctx context.Context) error {
	if pinger, ok := c.inner.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

func (c *sqlConn) ResetSession(ctx context.Context) error {
	if resetter, ok := c.inner.(driver.SessionResetter); ok {
		return resetter.ResetSession(ctx)
	}
	return nil
}

func (c *sqlConn) IsValid() bool {
	if validator, ok := c.inner.(driver.Validator); ok {
		return validator.IsValid()
	}
	return true
}

func (c *sqlConn) CheckNamedValue(value *driver.NamedValue) error {
	if checker, ok := c.inner.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

type sqlStmt struct {
	inner driver.Stmt
	query string
}

func (s *sqlStmt) Close() error  { return s.inner.Close() }
func (s *sqlStmt) NumInput() int { return s.inner.NumInput() }

// Exec is required by driver.Stmt; database/sql uses ExecContext.
func (s *sqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	started := time.Now()
	result, err := s.inner.Exec(args)
	recordQuery(context.Background(), s.query, started, err)
	return result, err
}

// Query is required by driver.Stmt; database/sql uses QueryContext.
func (s *sqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	started := time.Now()
	rows, err := s.inner.Query(args)
	recordQuery(context.Background(), s.query, started, err)
	return rows, err
}

func (s *sqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := s.inner.(driver.StmtExecContext)
	if !ok {
		values, err := plainValues(args)
		if err != nil {
			return nil, err
		}
		return s.Exec(values)
	}
	started := time.Now()
	result, err := execer.ExecContext(ctx, args)
	recordQuery(ctx, s.query, started, err)
	return result, err
}

func (s *sqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := s.inner.(driver.StmtQueryContext)
	if !ok {
		values, err := plainValues(args)
		if err != nil {
			return nil, err
		}
		return s.Query(values)
	}
	started := time.Now()
	rows, err := queryer.QueryContext(ctx, args)
	recordQuery(ctx, s.query, started, err)
	return rows, err
}

func (s *sqlStmt) CheckNamedValue(value *driver.NamedValue) error {
	if checker, ok := s.inner.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

// plainValues turns named values into positional ones for a driver without
// context support, as database/sql itself does.
func plainValues(named []driver.NamedValue) ([]driver.Value, error) {
	values := make([]driver.Value, len(named))
	for i, value := range named {
		if value.Name != "" {
			return nil, errors.New("bugfree: the driver does not support named parameters")
		}
		values[i] = value.Value
	}
	return values, nil
}
