package csql

import "database/sql"

func ForRow(rows *sql.Rows, do func(RowScanner)) {
	defer func() {
		Panic(rows.Close())
	}()
	for rows.Next() {
		do(rows)
	}
	Panic(rows.Err())
}
