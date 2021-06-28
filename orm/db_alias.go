// Copyright 2014 beego Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package orm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
)

// DriverType database driver constant int.
type DriverType int

// Enum the Database driver
const (
	_          DriverType = iota // 驱动类型枚举
	DRMySQL                      // mysql
	DRSqlite                     // sqlite
	DROracle                     // oracle
	DRPostgres                   // pgsql
	DRTiDB                       // TiDB
)

// database driver string.
type driver string

// get type constant int of current driver..
func (d driver) Type() DriverType {
	a, _ := dataBaseCache.get(string(d))
	return a.Driver
}

// get name of current driver
func (d driver) Name() string {
	return string(d)
}

// check driver iis implemented Driver interface or not.
var _ Driver = new(driver)

var (
	dataBaseCache = &_dbCache{cache: make(map[string]*alias)}
	drivers       = map[string]DriverType{
		"mysql":    DRMySQL,
		"postgres": DRPostgres,
		"sqlite3":  DRSqlite,
		"tidb":     DRTiDB,
		"oracle":   DROracle,
		"oci8":     DROracle, // github.com/mattn/go-oci8
		"ora":      DROracle, //https://github.com/rana/ora
	}
	dbBasers = map[DriverType]dbBaser{
		DRMySQL:    newdbBaseMysql(),
		DRSqlite:   newdbBaseSqlite(),
		DROracle:   newdbBaseOracle(),
		DRPostgres: newdbBasePostgres(),
		DRTiDB:     newdbBaseTidb(),
	}
)

// database alias cacher.
type _dbCache struct {
	mux   sync.RWMutex
	cache map[string]*alias
}

// add database alias with original name.
func (ac *_dbCache) add(name string, al *alias) (added bool) {
	ac.mux.Lock()
	defer ac.mux.Unlock()
	if _, ok := ac.cache[name]; !ok {
		ac.cache[name] = al
		added = true
	}
	return
}

// get database alias if cached.
func (ac *_dbCache) get(name string) (al *alias, ok bool) {
	ac.mux.RLock()
	defer ac.mux.RUnlock()
	al, ok = ac.cache[name]
	return
}

// get default alias.
func (ac *_dbCache) getDefault() (al *alias) {
	al, _ = ac.get("default")
	return
}

// 数据库类型
type DB struct {
	*sync.RWMutex
	DB             *sql.DB
	stmtDecorators *lru.Cache
}

func (d *DB) Begin() (*sql.Tx, error) {
	return d.DB.Begin()
}

func (d *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return d.DB.BeginTx(ctx, opts)
}

//su must call release to release *sql.Stmt after using
func (d *DB) getStmtDecorator(query string) (*stmtDecorator, error) {
	d.RLock()
	c, ok := d.stmtDecorators.Get(query)
	if ok {
		c.(*stmtDecorator).acquire()
		d.RUnlock()
		return c.(*stmtDecorator), nil
	}
	d.RUnlock()

	d.Lock()
	c, ok = d.stmtDecorators.Get(query)
	if ok {
		c.(*stmtDecorator).acquire()
		d.Unlock()
		return c.(*stmtDecorator), nil
	}

	stmt, err := d.Prepare(query)
	if err != nil {
		d.Unlock()
		return nil, err
	}
	sd := newStmtDecorator(stmt)
	sd.acquire()
	d.stmtDecorators.Add(query, sd)
	d.Unlock()

	return sd, nil
}

func (d *DB) Prepare(query string) (*sql.Stmt, error) {
	return d.DB.Prepare(query)
}

func (d *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return d.DB.PrepareContext(ctx, query)
}

func (d *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	sd, err := d.getStmtDecorator(query)
	if err != nil {
		return nil, err
	}
	stmt := sd.getStmt()
	defer sd.release()
	return stmt.Exec(args...)
}

func (d *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	sd, err := d.getStmtDecorator(query)
	if err != nil {
		return nil, err
	}
	stmt := sd.getStmt()
	defer sd.release()
	return stmt.ExecContext(ctx, args...)
}

func (d *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	sd, err := d.getStmtDecorator(query)
	if err != nil {
		return nil, err
	}
	stmt := sd.getStmt()
	defer sd.release()
	return stmt.Query(args...)
}

func (d *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	sd, err := d.getStmtDecorator(query)
	if err != nil {
		return nil, err
	}
	stmt := sd.getStmt()
	defer sd.release()
	return stmt.QueryContext(ctx, args...)
}

func (d *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	sd, err := d.getStmtDecorator(query)
	if err != nil {
		panic(err)
	}
	stmt := sd.getStmt()
	defer sd.release()
	return stmt.QueryRow(args...)

}

func (d *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	sd, err := d.getStmtDecorator(query)
	if err != nil {
		panic(err)
	}
	stmt := sd.getStmt()
	defer sd.release()
	return stmt.QueryRowContext(ctx, args)
}

// 别名类型
type alias struct {
	Name         string     // 名称
	Driver       DriverType // 驱动类型 例如：mysql
	DriverName   string     // 驱动名称
	DataSource   string     // 连接数据库语句
	MaxIdleConns int        // 最大空闲连接数
	MaxOpenConns int        // 最大连接数
	DB           *DB
	DbBaser      dbBaser
	TZ           *time.Location
	Engine       string
}

// 设置时区
func detectTZ(al *alias) {
	// ORM时区系统与数据库匹配
	// 默认使用 Local
	al.TZ = DefaultTimeLoc

	if al.DriverName == "sphinx" {
		return
	}

	switch al.Driver {
	case DRMySQL:
		// mysql  获取世界标准时间
		row := al.DB.QueryRow("SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP)")
		var tz string
		row.Scan(&tz)
		if len(tz) >= 8 {
			if tz[0] != '-' {
				tz = "+" + tz
			}
			t, err := time.Parse("-07:00:00", tz)
			if err == nil {
				if t.Location().String() != "" {
					al.TZ = t.Location()
				}
			} else {
				DebugLog.Printf("Detect DB timezone: %s %s\n", tz, err.Error())
			}
		}

		// 从当前数据库中获取默认引擎
		row = al.DB.QueryRow("SELECT ENGINE, TRANSACTIONS FROM information_schema.engines WHERE SUPPORT = 'DEFAULT'")
		var engine string
		var tx bool
		row.Scan(&engine, &tx)

		if engine != "" {
			al.Engine = engine
		} else {
			al.Engine = "INNODB"
		}

	case DRSqlite, DROracle:
		// sqlite oracle
		al.TZ = time.UTC

	case DRPostgres:
		// postgresql
		row := al.DB.QueryRow("SELECT current_setting('TIMEZONE')")
		var tz string
		row.Scan(&tz)
		loc, err := time.LoadLocation(tz)
		if err == nil {
			al.TZ = loc
		} else {
			DebugLog.Printf("Detect DB timezone: %s %s\n", tz, err.Error())
		}
	}
}

func addAliasWthDB(aliasName, driverName string, db *sql.DB) (*alias, error) {
	// 创建一个别名
	al := new(alias)
	al.Name = aliasName
	al.DriverName = driverName

	al.DB = &DB{
		RWMutex:        new(sync.RWMutex),
		DB:             db,
		stmtDecorators: newStmtDecoratorLruWithEvict(),
	}

	if dr, ok := drivers[driverName]; ok {
		al.DbBaser = dbBasers[dr]
		al.Driver = dr
	} else {
		return nil, fmt.Errorf("driver name `%s` have not registered", driverName)
	}

	// Ping验证与数据库的连接仍然有效，如果有必要，建立一个连接。
	err := db.Ping()
	if err != nil {
		return nil, fmt.Errorf("register db Ping `%s`, %s", aliasName, err.Error())
	}

	// 将别名写入缓存
	if !dataBaseCache.add(aliasName, al) {
		return nil, fmt.Errorf("DataBase alias name `%s` already registered, cannot reuse", aliasName)
	}

	return al, nil
}

// AddAliasWthDB为drivename添加一个别名。
func AddAliasWthDB(aliasName, driverName string, db *sql.DB) error {
	_, err := addAliasWthDB(aliasName, driverName, db)
	return err
}

// RegisterDataBase 设置数据库连接参数。使用数据库驱动程序自带的dataSource args。
func RegisterDataBase(aliasName, driverName, dataSource string, params ...int) error {
	var (
		err error
		db  *sql.DB
		al  *alias
	)
	// Open可能只是验证其参数，而不创建与数据库的连接到数据库的连接
	db, err = sql.Open(driverName, dataSource)
	if err != nil {
		err = fmt.Errorf("register db `%s`, %s", aliasName, err.Error())
		goto end
	}

	// 返回一个别名对象
	al, err = addAliasWthDB(aliasName, driverName, db)
	if err != nil {
		goto end
	}

	al.DataSource = dataSource

	// 校准时间
	detectTZ(al)

	// 设置连接池
	for i, v := range params {
		switch i {
		case 0:
			SetMaxIdleConns(al.Name, v)
		case 1:
			SetMaxOpenConns(al.Name, v)
		}
	}

	// 出现异常时都执行这个 主要是关闭数据库连接
end:
	if err != nil {
		if db != nil {
			db.Close()
		}
		DebugLog.Println(err.Error())
	}

	return err
}

// RegisterDriver 注册一个数据库驱动程序，使用指定的驱动程序名称，这可以定义该驱动程序是哪个数据库类型。
func RegisterDriver(driverName string, typ DriverType) error {
	if t, ok := drivers[driverName]; !ok {
		drivers[driverName] = typ
	} else {
		if t != typ {
			return fmt.Errorf("driverName `%s` db driver already registered and is other type", driverName)
		}
	}
	return nil
}

// SetDataBaseTZ Change the database default used timezone
func SetDataBaseTZ(aliasName string, tz *time.Location) error {
	if al, ok := dataBaseCache.get(aliasName); ok {
		al.TZ = tz
	} else {
		return fmt.Errorf("DataBase alias name `%s` not registered", aliasName)
	}
	return nil
}

// SetMaxIdleConns 改变*sql.DB的最大空闲连接，使用指定的数据库别名
func SetMaxIdleConns(aliasName string, maxIdleConns int) {
	// 获取别名对象
	al := getDbAlias(aliasName)
	al.MaxIdleConns = maxIdleConns
	al.DB.DB.SetMaxIdleConns(maxIdleConns)
}

// SetMaxOpenConns 改变*sql.DB的最大开放连接，使用指定的数据库别名
func SetMaxOpenConns(aliasName string, maxOpenConns int) {
	al := getDbAlias(aliasName)
	al.MaxOpenConns = maxOpenConns
	al.DB.DB.SetMaxOpenConns(maxOpenConns)
	// TODO 原因 for tip go 1.2
	if fun := reflect.ValueOf(al.DB).MethodByName("SetMaxOpenConns"); fun.IsValid() {
		fun.Call([]reflect.Value{reflect.ValueOf(maxOpenConns)})
	}
}

// GetDB Get *sql.DB from registered database by db alias name.
// Use "default" as alias name if you not set.
func GetDB(aliasNames ...string) (*sql.DB, error) {
	var name string
	if len(aliasNames) > 0 {
		name = aliasNames[0]
	} else {
		name = "default"
	}
	al, ok := dataBaseCache.get(name)
	if ok {
		return al.DB.DB, nil
	}
	return nil, fmt.Errorf("DataBase of alias name `%s` not found", name)
}

type stmtDecorator struct {
	wg      sync.WaitGroup
	lastUse int64
	stmt    *sql.Stmt
}

func (s *stmtDecorator) getStmt() *sql.Stmt {
	return s.stmt
}

func (s *stmtDecorator) acquire() {
	s.wg.Add(1)
	s.lastUse = time.Now().Unix()
}

func (s *stmtDecorator) release() {
	s.wg.Done()
}

//garbage recycle for stmt
func (s *stmtDecorator) destroy() {
	go func() {
		s.wg.Wait()
		_ = s.stmt.Close()
	}()
}

func newStmtDecorator(sqlStmt *sql.Stmt) *stmtDecorator {
	return &stmtDecorator{
		stmt:    sqlStmt,
		lastUse: time.Now().Unix(),
	}
}

// 创建一个缓存对象
func newStmtDecoratorLruWithEvict() *lru.Cache {
	cache, _ := lru.NewWithEvict(1000, func(key interface{}, value interface{}) {
		value.(*stmtDecorator).destroy()
	})
	return cache
}
