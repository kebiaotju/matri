package mysqldb

import (
	"database/sql"
)

//MysqlDB is a sql.DB, special for mysql
type MysqlDB sql.DB

//CreateMysqlDB Conn with db
func CreateMysqlDB(username string, password string, addr string, dbname string, charset string) (*MysqlDB, error) {
	db, err := Dail(username, password, addr, dbname, charset)
	if err != nil {
		return nil, err
	}
	return (*MysqlDB)(db), nil
}

//Base return inner sql.DB
func (r *MysqlDB) Base() *sql.DB {
	return (*sql.DB)(r)
}

//Close connection, error equal to sql.DB.Close
func (r *MysqlDB) Close() error {
	return (*sql.DB)(r).Close()
}

//Ping connection, error equal to sql.DB.Ping
func (r *MysqlDB) Ping() error {
	return (*sql.DB)(r).Ping()
}

//Insert a row, and return it`s id
func (r *MysqlDB) Insert(sqlstr string, args ...interface{}) (int64, error) {
	return Insert((*sql.DB)(r), sqlstr, args...)
}

//Exec update or delete, and return it affect row count
func (r *MysqlDB) Exec(sqlstr string, args ...interface{}) (int64, error) {
	return Exec((*sql.DB)(r), sqlstr, args...)
}

//QueryOne same as select, but only fetch one row
func (r *MysqlDB) QueryOne(sqlstr string, args ...interface{}) (map[string][]byte, error) {
	row, err := QueryRow((*sql.DB)(r), sqlstr, args...)
	if row == nil{
		return nil ,err
	}
	return *row, err
}

//Query same as select, map[string][]byte is row
func (r *MysqlDB) Query(sqlstr string, args ...interface{}) ([]map[string][]byte, error) {
	rows, err := QueryRows((*sql.DB)(r), sqlstr, args...)
	if rows == nil{
		return nil ,err
	}
	return *rows, err
}
