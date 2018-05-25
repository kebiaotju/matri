package mysqldb_test

import (
	"fmt"
	"github.com/qumi/matrix/db/mysqldb"
)

func Example() {
	db, err := mysqldb.Dail("root", "root123", "192.168.1.249:3306", "account_zhajinhua", "utf8")
	if err != nil {
		fmt.Printf("mysql connect error! ", err)
		return
	}

	last_insert_id, err := mysqldb.Insert(db, "insert into a_acc_index set phoneimei = ?", "fwef3gtf")
	fmt.Printf("insert id:", last_insert_id)

	ret, err := mysqldb.Exec(db, "update s_black_id set op_type = ? where acc_id = ?", 2, 2)
	fmt.Printf("exec sql result: ", ret)

	row, err := mysqldb.QueryRow(db, "select * from s_black_id limit 1")
	for k1, v1 := range *row {
		fmt.Printf(k1, v1)

	}

	rows, err := mysqldb.QueryRows(db, "select * from s_black_id limit 10")
	for _, v := range *rows {
		for key, value := range v {
			fmt.Printf(key, value)
		}
	}
}
