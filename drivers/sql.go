package drivers

import (
	"context"
	"database/sql"
	"errors"
	"github.com/jinglov/gomisc/monitor"
	"time"
)

type SQL struct {
	name       string
	Db         *sql.DB
	monitorVec *monitor.SqlVec
}

var (
	ErrDatasourcenameNil = errors.New("dataSourceName is nil")
)

func NewSql(monitor *monitor.SqlVec, driverName string, args ...string) (*SQL, error) {
	if len(args) == 0 {
		return nil, ErrDatasourcenameNil
	}
	var dataSourceName string
	name := "default"
	dataSourceName = args[0]
	if len(args) > 1 {
		name = args[1]
	}
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(time.Hour)

	return &SQL{Db: db, name: name, monitorVec: monitor}, nil
}

func (d *SQL) SetMaxIdleConns(n int) {
	d.Db.SetMaxIdleConns(n)
}

func (d *SQL) SetMaxOpenConns(n int) {
	d.Db.SetMaxOpenConns(n)
}

func (d *SQL) Ping() error {
	return d.Db.Ping()
}

func (d *SQL) Close() error {
	return d.Db.Close()
}

func (d *SQL) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	defer d.Monitor("exec", time.Now())
	return d.Db.ExecContext(ctx, query, args...)
}

func (d *SQL) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	defer d.Monitor("query", time.Now())
	return d.Db.QueryContext(ctx, query, args...)
}

func (d *SQL) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	defer d.Monitor("queryRow", time.Now())
	return d.Db.QueryRowContext(ctx, query, args...)
}

func (d *SQL) Begin() (*sql.Tx, error) {
	return d.Db.Begin()
}

func (d *SQL) Monitor(action string, start time.Time) {
	if d.monitorVec != nil {
		timeConsuming := time.Since(start)
		d.monitorVec.ObServe(&monitor.SqlLabels{Name: d.name, Action: action}, float64(timeConsuming)/float64(time.Millisecond))
	}
}
