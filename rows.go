package csql

import "database/sql"

func ForRow(rows *sql.Rows, do func(RowScanner)) (err error) {
	defer Safe(&err)
	defer rows.Close()

	for rows.Next() {
		do(rows)
	}
	Panic(rows.Err())
	return
}
