package g

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

func InitMysql(acc, pw, addr, port, database string) error {
	var err error
	// 初始化mysql连接
	sqlConn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", acc, pw, addr, port, database)
	DB, err = sqlx.Open("mysql", sqlConn)
	if err != nil {
		return err
	}

	// 测试db是否正常
	err = DB.Ping()
	if err != nil {
		return err
	}

	return nil
}
