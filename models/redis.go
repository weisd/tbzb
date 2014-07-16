package models

import(
    "time"
    "fmt"
    "errors"
    "strconv"
    "github.com/weisd/tbzb/base"
    "github.com/garyburd/redigo/redis"
)

var (
    RedisPool *redis.Pool
)

func RedisInit(){
    server  := base.Cfg.MustValue("redis", "server", "127.0.0.1:6379")
    pwd  := base.Cfg.MustValue("redis", "password", "")

    RedisPool = NewRedis(server, pwd)
}

// redis 连接池
func NewRedis(server, pass string) *redis.Pool {
    return &redis.Pool{
        MaxIdle:3,
        IdleTimeout:240*time.Second,
        Dial:func()(redis.Conn, error){
            c, err := redis.Dial("tcp", server)
            if err != nil {
                return nil, err
            }

            if pass != "" {
	            if _, err := c.Do("AUTH", pass); err != nil {
	                c.Close()
	                return nil, err
	            }
            }

            return c,nil
        },
        TestOnBorrow:func(c redis.Conn, t time.Time) error{
            _, err := c.Do("PING")
            return err
        },
    }
}

func RedisSaveRecord(){

}



// 取杠杆
func GetLever(conn redis.Conn, symbol_pre string) (int64, error) {
    return redis.Int64(conn.Do("HGET", "futures.strategy.symbol.lever", symbol_pre))
}


// 同步 到redis
func Mysql2Redis(mysqlId int64) (err error){
    record := new(ZhiboRecord)
    ok, err := Engine.Id(mysqlId).Get(record)
    if err != nil || !ok {
        err = errors.New("mysql记录不存在:")
        return
    }

    conn := RedisPool.Get()
    defer conn.Close()

    timeStr := record.Time.Format("20060102150405")

    // 是否已存在
    ok , err = RedisCheckExists(record.FormulaName, record.Symbol, record.Action, timeStr)
    if err != nil{
        return
    }

    if ok {
        base.Warn("redis记录已存在")
        return nil
    }


    rid, err := GetRecordFeedID()
    if err != nil {
        return
    }

    key := fmt.Sprintf("futures.live.result:%s", rid)

    // 写入info
    base.Info("redis record key:", key)

    args := []interface{}{key} 

    args = append(args, "id", rid)
    args = append(args, "formula_name", record.FormulaName)
    args = append(args, "symbol", record.Symbol)
    args = append(args, "symbol_pre", record.SymbolPre)
    args = append(args, "time", record.Time.Format("2006-01-02 15:04:05"))
    args = append(args, "action", record.Action)
    args = append(args, "number", strconv.FormatInt(record.Number, 10))
    args = append(args, "price", fmt.Sprintf("%.2f", record.Price))
    args = append(args, "entry_price", fmt.Sprintf("%.2f", record.EntryPrice))
    args = append(args, "now_position", fmt.Sprintf("%d", record.NowPosition))
    args = append(args, "bar_num", fmt.Sprintf("%d",record.BarNum))
    args = append(args, "profit", fmt.Sprintf("%.2f", record.Profit))
    args = append(args, "is_profit", strconv.FormatBool(record.IsProfit))
    args = append(args, "lever", fmt.Sprintf("%d",record.Lever))
    args = append(args, "add_time", record.AddTime.Format("2006-01-02 15:04:05"))
    


    _ , err = redis.String(conn.Do("HMSET", args...))
    if err != nil{
        return
    }
        base.Info("写入reids hash成功")

    // 写入记录总表
    ok , err = RedisSave2AllRecord(record.Time.Unix(), rid)
    if err != nil || !ok {
        return 
    }
        base.Info("写入reids 总记录成功")

    // 写入记录对应表
    ok , err = RedisSave2SymoblRecord(record.Time.Unix(), rid, record.SymbolPre)
    if err != nil || !ok {
        return 
    }
        base.Info("写入reids 类记录成功")
    // 写入已存在表
    ok, err = RedisSaveExists(record.FormulaName, record.SymbolPre, record.Action, record.Time.Format("20060102150405"))
    if err != nil || !ok {
        return 
    }
        base.Info("写入reids 已存在记录成功")

    return
}


// 取记录id
func GetRecordFeedID() (rid string, err error) {
    conn := RedisPool.Get()
    defer conn.Close()

    // 取id
    res, err := conn.Do("INCR", "feed:counter")
    if err != nil {
        base.Warn(err)
        return
    }

    id := res.(int64)
    rid = strconv.FormatInt(id, 10)

    return rid+"47", nil
}

// 写入记录总表
func RedisSave2AllRecord(time int64, rid string) (ok bool, err error) {
    conn := RedisPool.Get()
    conn.Close()

    return redis.Bool(conn.Do("ZADD", "futures.live.result:all", time, rid))
}

// 写入对应记录
func RedisSave2SymoblRecord(time int64, rid, pre string) (ok bool, err error) {
    conn := RedisPool.Get()
    conn.Close()

    key := fmt.Sprintf("futures.live.result:%s", pre)

    return redis.Bool(conn.Do("ZADD", key, time, rid))
}

// 记录已添加
func RedisSaveExists(fname, symbol, action, time string) (bool, error) {
    conn := RedisPool.Get()
    defer conn.Close()
    member := fmt.Sprintf("%s_%s_%s_%s", fname, symbol, action, time)
    return redis.Bool(conn.Do("SADD", "futures.live.result:record", member))
}

// 记录是否已存在redis
func RedisCheckExists(fname, symbol, action, time string) (bool, error) {
    conn := RedisPool.Get()
    defer conn.Close()

    member := fmt.Sprintf("%s_%s_%s_%s", fname, symbol, action, time)
    return redis.Bool(conn.Do("SISMEMBER", "futures.live.result:record", member))
    
}
