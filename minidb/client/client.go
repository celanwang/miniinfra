package client

import (
	"bufio"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
	"github.com/celanwang/miniinfra/minidb/internal/wire"
)

// Error codes distinguish failures a client can retry from a commit whose
// result must be reconciled using the application's request key.
const (
	Unique        = "unique"
	Deadlock      = "deadlock"
	LockWait      = "lock_wait"
	CommitUnknown = "commit_unknown"
)

type Error struct {
	Code    string
	Message string
}

func (err *Error) Error() string { return "minidb " + err.Code + ": " + err.Message }

// Open returns a database/sql pool for a MiniDB TCP listener.
func Open(address string) *sql.DB {
	return sql.OpenDB(connector{address: address})
}

type connector struct{ address string }
type networkDriver struct{}

func (c connector) Driver() driver.Driver { return networkDriver{} }
func (networkDriver) Open(address string) (driver.Conn, error) {
	return connector{address: address}.Connect(context.Background())
}
func (c connector) Connect(ctx context.Context) (driver.Conn, error) {
	dialer := net.Dialer{}
	socket, err := dialer.DialContext(ctx, "tcp", c.address)
	if err != nil {
		return nil, err
	}
	return &connection{socket: socket, reader: wire.Reader(socket)}, nil
}

type connection struct {
	socket net.Conn
	reader *bufio.Scanner
	broken bool
	inTx   bool
}

func (c *connection) Prepare(sql string) (driver.Stmt, error) {
	return &statement{c: c, sql: sql}, nil
}
func (c *connection) Close() error {
	c.broken = true
	return c.socket.Close()
}
func (c *connection) IsValid() bool { return !c.broken }
func (c *connection) ResetSession(ctx context.Context) error {
	if c.inTx {
		if _, err := c.request(ctx, "ROLLBACK", nil); err != nil {
			c.Close()
			return driver.ErrBadConn
		}
	}
	return nil
}

func (c *connection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *connection) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if options.ReadOnly || options.Isolation != 0 && options.Isolation != driver.IsolationLevel(sql.LevelReadCommitted) {
		return nil, fmt.Errorf("MiniDB supports read-write READ COMMITTED transactions")
	}
	if _, err := c.request(ctx, "BEGIN", nil); err != nil {
		return nil, err
	}
	return transaction{c: c}, nil
}

type transaction struct{ c *connection }

func (tx transaction) Commit() error {
	_, err := tx.c.request(context.Background(), "COMMIT", nil)
	return err
}
func (tx transaction) Rollback() error {
	_, err := tx.c.request(context.Background(), "ROLLBACK", nil)
	return err
}

func (c *connection) ExecContext(ctx context.Context, sql string, args []driver.NamedValue) (driver.Result, error) {
	response, err := c.request(ctx, sql, args)
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(response.Result.Affected), nil
}

func (c *connection) QueryContext(ctx context.Context, sql string, args []driver.NamedValue) (driver.Rows, error) {
	response, err := c.request(ctx, sql, args)
	if err != nil {
		return nil, err
	}
	return &rows{columns: response.Result.Columns, data: response.Result.Rows}, nil
}

func (c *connection) request(ctx context.Context, sql string, args []driver.NamedValue) (wire.Response, error) {
	if c.broken {
		return wire.Response{}, driver.ErrBadConn
	}
	if err := ctx.Err(); err != nil {
		return wire.Response{}, err
	}
	if !utf8.ValidString(sql) {
		return wire.Response{}, fmt.Errorf("MiniDB SQL must be valid UTF-8")
	}
	req := wire.Request{SQL: sql}
	for _, arg := range args {
		if arg.Name != "" {
			return wire.Response{}, fmt.Errorf("MiniDB uses positional ? parameters")
		}
		switch value := arg.Value.(type) {
		case nil:
			req.Args = append(req.Args, engine.Value{})
		case int64:
			req.Args = append(req.Args, engine.Int(value))
		case string:
			if !utf8.ValidString(value) {
				return wire.Response{}, fmt.Errorf("MiniDB text parameter %d must be valid UTF-8", arg.Ordinal)
			}
			req.Args = append(req.Args, engine.String(value))
		case bool:
			req.Args = append(req.Args, engine.Bool(value))
		default:
			return wire.Response{}, fmt.Errorf("unsupported MiniDB parameter type %T", value)
		}
	}
	data, err := wire.Encode(req)
	if err != nil {
		return wire.Response{}, err
	}
	fields := strings.Fields(sql)
	var verb string
	if len(fields) != 0 {
		verb = strings.TrimSuffix(strings.ToLower(fields[0]), ";")
	}
	deadline := time.Now().Add(40 * time.Second)
	if requested, ok := ctx.Deadline(); ok && requested.Before(deadline) {
		deadline = requested
	}
	if err := c.socket.SetDeadline(deadline); err != nil {
		c.Close()
		return wire.Response{}, err
	}
	stopped := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		c.socket.SetDeadline(time.Now())
		close(stopped)
	})
	defer func() {
		if !stop() {
			<-stopped
		}
	}()
	var response wire.Response
	if _, err := c.socket.Write(data); err != nil {
		return wire.Response{}, c.transportError(verb, err)
	}
	if err := wire.Read(c.reader, &response); err != nil {
		return wire.Response{}, c.transportError(verb, err)
	}
	if response.InTransaction != nil {
		c.inTx = *response.InTransaction
	} else if response.Code == "" {
		// Older servers omit the session state.
		switch verb {
		case "begin":
			c.inTx = true
		case "commit", "rollback":
			c.inTx = false
		}
	}
	if response.Code != "" {
		return wire.Response{}, &Error{Code: response.Code, Message: response.Message}
	}
	return response, nil
}

func (c *connection) transportError(verb string, err error) error {
	inTx := c.inTx
	c.Close()
	if verb == "commit" || !inTx && (verb == "insert" || verb == "update" || verb == "delete" || verb == "create") {
		return &Error{Code: CommitUnknown, Message: err.Error()}
	}
	return fmt.Errorf("MiniDB connection: %w", err)
}

type rows struct {
	columns []string
	data    [][]engine.Value
}

func (r *rows) Columns() []string { return r.columns }
func (r *rows) Close() error {
	r.data = nil
	return nil
}
func (r *rows) Next(out []driver.Value) error {
	if len(r.data) == 0 {
		return io.EOF
	}
	for i, field := range r.data[0] {
		switch field.Kind {
		case engine.Null:
			out[i] = nil
		case engine.Integer:
			out[i] = field.Int
		case engine.Text:
			out[i] = field.Text
		case engine.Boolean:
			out[i] = field.Int != 0
		}
	}
	r.data = r.data[1:]
	return nil
}

type statement struct {
	c   *connection
	sql string
}

func (*statement) Close() error  { return nil }
func (*statement) NumInput() int { return -1 }
func (s *statement) Exec(values []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), positional(values))
}
func (s *statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.c.ExecContext(ctx, s.sql, args)
}
func (s *statement) Query(values []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), positional(values))
}
func (s *statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.c.QueryContext(ctx, s.sql, args)
}
func positional(values []driver.Value) []driver.NamedValue {
	args := make([]driver.NamedValue, len(values))
	for i, value := range values {
		args[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
	}
	return args
}
