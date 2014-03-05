package csql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq"
)

type Inserter struct {
	tx      *sql.Tx
	table   string
	columns []string
	driver  string

	stmt *sql.Stmt
}

func NewInserter(
	tx *sql.Tx,
	driver string,
	table string,
	columns ...string,
) (ins *Inserter, err error) {
	defer Safe(&err)

	ins = &Inserter{tx, table, columns, driver, nil}
	if driver == "postgres" {
		ins.stmt = Prepare(tx, pq.CopyIn(table, columns...))
	} else {
		ins.stmt = ins.preparedInsert()
	}
	return
}

func (in *Inserter) Exec(args ...interface{}) error {
	if len(args) > 0 || in.driver == "postgres" {
		// postgresql requires this to clear the "COPY" buffer with
		// 0 arguments.
		// But obviously, this doesn't work for a simple prepared
		// statement.
		_, err := in.stmt.Exec(args...)
		if err != nil {
			return err
		}
	}
	if len(args) == 0 {
		return in.stmt.Close()
	}
	return nil
}

func (in *Inserter) preparedInsert() *sql.Stmt {
	params := make([]string, len(in.columns))
	for i := range in.columns {
		params[i] = fmt.Sprintf("$%d", i+1)
	}
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		in.table, strings.Join(in.columns, ", "), strings.Join(params, ", "))
	return Prepare(in.tx, q)
}
