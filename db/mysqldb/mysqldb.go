package mysqldb

import (
	"database/sql"

	"github.com/qumi/matrix/log"
	_ "github.com/go-sql-driver/mysql"
)

//connect mysql
func Dail(username string, password string, addr string, dbname string, charset string) (*sql.DB, error) {
	conn_str := username + ":" + password + "@tcp(" + addr + ")/" + dbname + "?charset=" + charset
	db, err := sql.Open("mysql", conn_str)
	if err != nil {
		panic(err.Error())

	}
	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}
	return db, err
}

//insert
func Insert(db *sql.DB, sqlstr string, args ...interface{}) (int64, error) {
	stmtIns, err := db.Prepare(sqlstr)
	if err != nil {
		log.Error("sql:%s,%s", sqlstr, err.Error())
		return 0, err
	}
	defer stmtIns.Close()

	result, err := stmtIns.Exec(args...)
	if err != nil {
		log.Error("sql:%s,%s", sqlstr, err.Error())
		return 0, err
	}
	return result.LastInsertId()
}

//update or delete
func Exec(db *sql.DB, sqlstr string, args ...interface{}) (int64, error) {
	stmtIns, err := db.Prepare(sqlstr)
	if err != nil {
		log.Error("sql:%s,%s", sqlstr, err.Error())
		return 0, err
	}
	defer stmtIns.Close()

	result, err := stmtIns.Exec(args...)
	if err != nil {
		log.Error("sql:%s,%s", sqlstr, err.Error())
		return 0, err
	}
	return result.RowsAffected()
}

//query one row
func QueryRow(db *sql.DB, sqlstr string, args ...interface{}) (*map[string][]byte, error) {
	stmtOut, err := db.Prepare(sqlstr)
	if err != nil {
		log.Error("sql:%s,%s", sqlstr, err.Error())
		return nil, err
	}
	defer stmtOut.Close()

	rows, err := stmtOut.Query(args...)
	if err != nil {
		log.Error("sql:%s,%s", sqlstr, err.Error())
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Error("sql:%s,%s", sqlstr, err.Error())
		return nil, err
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	ret := make(map[string][]byte, len(scanArgs))

	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error())
		}
		var value []byte

		for i, col := range values {
			if col == nil {
				value = []byte{'N', 'U', 'L', 'L'}
			} else {
				value = col
			}
			ret[columns[i]] = value
		}
		break //get the first row only
	}
	return &ret, nil
}

//query rows
func QueryRows(db *sql.DB, sqlstr string, args ...interface{}) (*[]map[string][]byte, error) {
	stmtOut, err := db.Prepare(sqlstr)
	if err != nil {
		log.Error("sql:%s,%s", sqlstr, err.Error())
		return nil, err
	}
	defer stmtOut.Close()

	rows, err := stmtOut.Query(args...)
	if err != nil {
		log.Error("sql:%s,%s", sqlstr, err.Error())
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Error("sql:%s,%s", sqlstr, err.Error())
		return nil, err
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))

	ret := make([]map[string][]byte, 0, 32)
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error())
		}
		vmap := make(map[string][]byte, len(scanArgs))
		for i, col := range values {
			var value []byte
			if col == nil {
				value = make([]byte, 4)
				copy(value, []byte{'N', 'U', 'L', 'L'})
			} else {
				value = make([]byte, len(col))
				copy(value, col)
			}
			vmap[columns[i]] = value
		}
		ret = append(ret, vmap)
	}
	return &ret, nil
}
