package csql

import "database/sql"

func ForRow(rows *sql.Rows, do func(RowScanner)) error {
	return Safe(func() {
		defer rows.Close()
		for rows.Next() {
			do(rows)
		}
		SQLPanic(rows.Err())
	})
}
