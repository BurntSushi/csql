package csql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq"
)

// TODO: Modify this so it uses COPY IN from github.com/lib/pq when it's
// available.
//
// But how do we know if the pq package is being used? Hmm...

type Inserter struct {
	tx            *sql.Tx
	current, size int
	table         string
	columns       []string

	stmt *sql.Stmt
	data []interface{}
}

func NewInserter(
	tx *sql.Tx,
	driver string,
	size int,
	table string,
	columns ...string,
) (ins *Inserter, err error) {
	defer Safe(&err)

	if size < 1 {
		size = 1
	}
	ins = &Inserter{tx, 0, size, table, columns, nil, nil}
	if driver == "postgres" {
		ins.stmt = Prepare(tx, pq.CopyIn(table, columns...))
	} else {
		ins.data = make([]interface{}, 0, 100)
	}
	return
}

func (in *Inserter) Exec(args ...interface{}) error {
	if in.stmt != nil {
		_, err := in.stmt.Exec(args...)
		if err != nil {
			return err
		}
		if len(args) == 0 {
			return in.stmt.Close()
		}
		return nil
	}
	if len(args) == 0 {
		return in.run()
	}
	if len(args) != len(in.columns) {
		panic("len(args) != len(columns)")
	}
	in.data = append(in.data, args...)
	in.current++
	if in.current == in.size {
		return in.run()
	}
	return nil
}

func (in *Inserter) run() (err error) {
	defer Safe(&err)

	q := in.prepare()
	if len(q) == 0 {
		return // nothing to insert! success!
	}

	Exec(in.tx, q, in.data...)
	in.current = 0
	in.data = in.data[:0]
	return
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
		rows[i] = makePlaceholders(1 + i*len(in.columns))
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		in.table, strings.Join(in.columns, ", "), strings.Join(rows, ", "))
}
