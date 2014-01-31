package csql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
)

// Execer describes values that can execute queries without returning any rows.
type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// Queryer describes values that can run queries which return 1 or many rows.
type Queryer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// QExecer is the composition of the Queryer and Execer interfaces.
type QExecer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// Preparer describes values that can create prepared statements.
type Preparer interface {
	Prepare(query string) (*sql.Stmt, error)
}

// RowScanner describes values that can scan a row of values.
type RowScanner interface {
	Scan(dest ...interface{}) error
}

// Valuer describes values that can convert themselves to a driver value.
type Valuer interface {
	Value() (driver.Value, error)
}

// Beginner describes values that can begin a transaction.
type Beginner interface {
	Begin() (*sql.Tx, error)
}

// SQLError satisfies the error interface. All panic'd errors in this package
// are SQLErrors. Errors returned by functions in this package are never
// SQLerrors.
type SQLError struct {
	error
}

func (se SQLError) Error() string {
	return se.error.Error()
}

// Safe executes any function that may panic with a SQLError safely. In
// particular, if `f` panics with a SQLError, then Safe recovers and returns
// the error wrapped by SQLError.
//
// If `f` panics with any other type of error, the panic is not recovered.
func Safe(f func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(SQLError); !ok {
				panic(r)
			} else {
				err = e.error
				return
			}
		}
	}()
	f()
	return nil
}

// Panic will wrap the given error in SQLError and pass it to panic.
// If the error is nil, this function does nothing.
func Panic(err error) {
	if err != nil {
		panic(SQLError{err})
	}
}

// Tx runs the given function safely within a transaction. If the function
// panics with a SQLError, then the transaction is rolled back. Otherwise,
// the transaction is committed.
//
// The first error that occurs (including beginning and ending the transaction)
// is returned.
func Tx(db Beginner, f func()) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if err := Safe(f); err != nil {
		tx.Rollback() // ignore this error (return the first)
		return err
	}
	return tx.Commit()
}

// Exec returns the result of a running a query that doesn't return any rows.
// If an error occurs, it is panic'd as a SQLError.
func Exec(db Execer, query string, args ...interface{}) sql.Result {
	r, err := db.Exec(query, args...)
	Panic(err)
	return r
}

// Query returns the result of a query that fetches many rows.
// If an error occurs, it is panic'd as a SQLError.
func Query(db Queryer, query string, args ...interface{}) *sql.Rows {
	rs, err := db.Query(query, args...)
	Panic(err)
	return rs
}

// Prepare returns a prepared statement.
// If an error occurs, it is panic'd as a SQLError.
func Prepare(db Preparer, query string) *sql.Stmt {
	stmt, err := db.Prepare(query)
	Panic(err)
	return stmt
}

// Scan performs a scan on a row. If an error occurs, it is panic'd as a
// SQLError.
func Scan(scanner RowScanner, dest ...interface{}) {
	Panic(scanner.Scan(dest...))
}

// Value returns the driver value.
// If an error occurs, it is panic'd as a SQLError.
func Value(v Valuer) driver.Value {
	dval, err := v.Value()
	Panic(err)
	return dval
}

// Count accepts any query of the form "SELECT COUNT(*) FROM ..." and returns
// the count. If an error occurs, it is panic'd as a SQLError.
//
// Any args given are passed to the query.
func Count(db Queryer, query string, args ...interface{}) int {
	var count int
	Scan(db.QueryRow(query, args...), &count)
	return count
}

// Truncate truncates the table given. It uses the driver given to determine
// what kind of query to run.
func Truncate(db Execer, driver, table string) error {
	return Safe(func() {
		if driver == "sqlite3" {
			Exec(db, fmt.Sprintf("DELETE FROM %s", table))
		} else {
			Exec(db, fmt.Sprintf("TRUNCATE TABLE %s", table))
		}
	})
}
