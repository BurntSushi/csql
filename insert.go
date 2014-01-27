package csql

import (
	"database/sql"
	"fmt"
	"strings"
)

type PrepExecer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
}

type Inserter struct {
	db            PrepExecer
	current, size int
	table         string
	columns       []string
	data          []interface{}
}

func NewInserter(
	db PrepExecer,
	size int,
	table string,
	columns ...string) *Inserter {
	if size < 1 {
		size = 1
	}
	return &Inserter{db, 0, size, table, columns, make([]interface{}, 0, 100)}
}

func (in *Inserter) Add(args ...interface{}) error {
	if len(args) != len(in.columns) {
		panic("len(args) != len(columns)")
	}
	in.data = append(in.data, args...)
	in.current++
	if in.current == in.size {
		return in.Exec()
	}
	return nil
}

func (in *Inserter) Exec() error {
	err := Safe(func() {
		q := in.prepare()
		if len(q) == 0 {
			return // nothing to insert! success!
		}
		Exec(in.db, q, in.data...)
	})
	in.current = 0
	in.data = in.data[:0]
	return err
}

func (in *Inserter) prepare() string {
	if in.current == 0 {
		return ""
	}

	makePlaceholders := func(start int) string {
		oneInsert := make([]string, len(in.columns))
		for i := range oneInsert {
			oneInsert[i] = fmt.Sprintf("$%d", start+i)
		}
		return fmt.Sprintf("(%s)", strings.Join(oneInsert, ", "))
	}

	rows := make([]string, in.current)
	for i := range rows {
		rows[i] = makePlaceholders(1 + i * len(in.columns))
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		in.table, strings.Join(in.columns, ", "), strings.Join(rows, ", "))
}
