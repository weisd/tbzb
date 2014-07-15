package models

import(
    "fmt"
    "time"
    "errors"
    "strconv"
    "math"
    "regexp"
    "github.com/Unknwon/goconfig"
    "github.com/weisd/tblog/conf"
   _ "github.com/go-sql-driver/mysql"
   "github.com/go-xorm/xorm"
    "github.com/garyburd/redigo/redis"
    "github.com/weisd/tblog/helper"
)

var (
    Cfg *goconfig.ConfigFile
    Engine *xorm.Engine
    RedisPool *redis.Pool
)

type Finfo struct {
    Id          int64     // ID
    FormulaName string    // 名称
    Symbol      string    // 交易品种
    Capital     float64   // 本金
    Remaining   float64   // 余额
    StartDate   time.Time // 开始时间
    LastDate    time.Time // 最新交易时间
    JingZhi     float64   // 净值
    JingLiRun   float64   // 净利润
    SumYingLi   float64   // 总盈利
    MaxYingLi   float64   // 最大盈利
    SumKuiSun   float64   // 总亏损
    MaxKuiSun   float64   // 最大亏损
    CountSellTimes    int64   // 交易次数
    CountYingLiTimes  int64   // 盈利次数
    CountKuiSunTimes  int64   // 亏损次数
    RateShengLv float64   // 胜率
    AvgChiCangBar     int64   // 平均持仓时间
    CountSellDay      int64   // 总交易天数
    AvgMonthShouYi    float64 // 月平均收益
    CountYingLiNumber int64   // 盈利手数
    CountKuiSunNumber int64   // 亏损手数
    SumNumber   int64     // 总交易手数
    AvgYingLi   float64   // 平均盈利
    AvgKuiSun   float64   // 平均亏损
    RateYingKui float64   // 盈亏比
    CounterYingLi     int64   // 盈利计数
    CounterKuiSun     int64   // 亏损计数
    MaxYingLiTimes    int64   //  最大连续盈利次数
    MaxKuiSunTimes    int64   // 最大连续亏损次数
    CountSellMonths   int64   // 交易月数
    RateShouYi        float64   // 收益率
    RateMonthShouYi   float64 // 月平均收益率
    RateYearShouYi    float64 // 年化收益率
    MaxJingLiRun      float64 // 最大净利润
    MaxHuiChePrice    float64 // 最大回撤
    RateMaxHuiChe     float64 // 最大回撤百分比
    RateYearShouYiMaxHuiChe   float64 // 年化收益率/最大回撤百分比
    UpdateTime  time.Time   // 记录更新时间
    Xiapu       float64
}

func init(){
    var err error
    Cfg, err = conf.NewCfg("./conf.ini")
    checkErr(err)

    user := Cfg.MustValue("mysql", "user")
    pass := Cfg.MustValue("mysql", "passwd")
    host := Cfg.MustValue("mysql", "host")
    port := Cfg.MustValue("mysql", "port")
    dbname := Cfg.MustValue("mysql", "dbname")
    charset := Cfg.MustValue("mysql", "charset")

    Engine, err = NewXorm(user, pass, dbname, host, port, charset)
    checkErr(err)


    server  := Cfg.MustValue("redis", "server")
    pwd  := Cfg.MustValue("redis", "password")

    RedisPool = NewRedis(server, pwd)
}

// mysql engine
func NewXorm(user, pass, dbname, host, port, charset string)(eg *xorm.Engine, err error){
    dns := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s", user, pass, host, port, dbname, charset)
    eg, err = xorm.NewEngine("mysql", dns)
    if err != nil {
        return
    }

    return
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

// 错误退出
func checkErr(err error){
    if err != nil {
        panic(err)
    }
}

// 交易记录
type TbRecord struct{
    Id string
    FormulaName string
    Symbol string
    Date string
    Time string
    Action string
    Number int32
    Price float64
    EntryPrice float64
    NowPosition int32
    Profit float64
    BarNum int32
    IsProfit int
}

// mysqlInfo是否存在
func CheckMysqlInfoExists(fname, symbol string) bool {
    fInfo := new(Finfo)

    has, err := Engine.Where("formula_name=? and symbol=?", fname, symbol).Get(fInfo)
    if err != nil || !has {
        fmt.Println(err, has)
        return false
    }

    return true
}

// 保存交易记录到mysql
func SaveTbRecord(info map[string]string) (err error){
    //tb_record

    formula_name, ok := info["FormulaName"]
    if !ok {
        return errors.New("[ERROR]field name FormulaName no exists!")
    }

    symbol, ok := info["Symbol"]
    if !ok {
        return errors.New("[ERROR]field name Symbol no exists!")
    }

    v, ok := info["date"]
    if !ok {
        return errors.New("[ERROR]field name date no exists!")
    }
    dateTime, err := time.Parse("20060102", v)
    if err != nil {
        return errors.New("[ERROR]field name date parse failed !")
    }
    dateStr := dateTime.Format("2006-01-02")

     v, ok = info["time"]
    if !ok {
        return errors.New("[ERROR]field name time no exists!")
    }
    f64, err := strconv.ParseFloat(v, 64);
    if err != nil {
        return errors.New("{ERROR failed to parsefloat time}")
    }
    timeTime, err := time.Parse("0.150405", fmt.Sprintf("%0.6f", f64))
    if err != nil {
        return errors.New("[ERROR]field name time parse failed !")
    }
    timeStr := timeTime.Format("15:04:05")

    v, ok = info["action"]
    if !ok {
        return errors.New("[ERROR]field name action no exists!")
    }
    action := v

    v, ok = info["number"]
    if !ok {
        return errors.New("[ERROR]field name number no exists!")
    }
    number, _ := strconv.Atoi(v)

    v, ok = info["price"]
    if !ok {
        return errors.New("[ERROR]field name price no exists!")
    }
    price,_ := strconv.ParseFloat(v, 64)

    v, ok = info["EntryPrice"]
    if !ok {
        return errors.New("[ERROR]field named EntryPrice no exists!")
    }
    entry_price,_ := strconv.ParseFloat(v, 64)

    v, ok = info["nowPosition"]
    if !ok {
        return errors.New("[ERROR]field named nowPosition no exists!")
    }
    now_position, _ := strconv.Atoi(v)

    v, ok = info["BarNum"]
    if !ok {
        return errors.New("[ERROR]field named BarNum no exists!")
    }
    bar_num, err := strconv.ParseFloat(v, 64)

    id := fmt.Sprintf("%s_%s_%s_%s_%s", formula_name, symbol, dateStr, timeStr, action)

    ex := &TbRecord{Id:id}
    has , err := Engine.Get(ex)
    if has {
        // @todo 已存在提示
        return
        return errors.New("record exists !!")
    }

    profit := 0.00
    // 添加品种杠杆
    conn := RedisPool.Get()
    defer conn.Close()

    // 品种字母

    ganggan, err := GetSymbolLever(conn, symbol)
    if err != nil {
        return errors.New(fmt.Sprintf("没有对应的品种杠杆:%s", symbol))
    }

    // 算出利润
    if action == "sell" {
        profit =  (price - entry_price) * float64(number) * float64(ganggan)
    } else if action == "buytocover" {
        profit = (entry_price - price) * float64(number) * float64(ganggan)
    }

    var isProfit int = 0
    if profit > 0 {
        isProfit = 3
    } else if profit < 0 {
        isProfit = 1
    } else {
        isProfit = 2
    }

    sql := "REPLACE INTO `tb_record`(`id`, `formula_name`, `symbol`, `date`, `time`, `action`, `number`, `price`, `entry_price`, `now_position`, `profit`, `is_profit`, `bar_num`) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    _, err = Engine.Exec(sql, id, formula_name, symbol, dateStr, timeStr, action, number, price, entry_price, now_position, profit, isProfit, bar_num)

    if err != nil {
        return
    }

   
    // 存到reids
    err = Record2Redis(conn, id, formula_name, symbol)
    if err != nil {
        fmt.Println("xxxx", err)
        return
    }

    if isProfit == 2 {
        return
    }

    // 查出info

    fInfo := new(Finfo)

    has, err = Engine.Where("formula_name=? and symbol=?", formula_name, symbol).Get(fInfo)
    if err != nil || !has {
        return
    }

    updateInfo := new(Finfo)

    updateInfo.LastDate = fInfo.LastDate
    updateInfo.CounterKuiSun = fInfo.CounterKuiSun
    updateInfo.CounterYingLi = fInfo.CounterYingLi
    updateInfo.MaxKuiSunTimes = fInfo.MaxKuiSunTimes
    updateInfo.MaxYingLiTimes = fInfo.MaxYingLiTimes

    // 最后交易 时间
    lastDate := fInfo.LastDate
    insertDate, err := time.Parse("2006-01-02", dateStr)
    if err != nil {
        return
    }

    // 如果大于最新时间则更新
    if insertDate.After(lastDate) {
        updateInfo.LastDate = insertDate
    }

    // 最大连续盈/亏
    if isProfit == 1 {
        updateInfo.CounterKuiSun = fInfo.CounterKuiSun + 1
        updateInfo.CounterYingLi = 0

        if updateInfo.CounterKuiSun > fInfo.MaxKuiSunTimes {
            updateInfo.MaxKuiSunTimes = updateInfo.CounterKuiSun
        }

    } else if isProfit == 3 {
        updateInfo.CounterKuiSun = 0
        updateInfo.CounterYingLi = fInfo.CounterYingLi + 1
        if updateInfo.CounterYingLi > fInfo.MaxYingLiTimes {
            updateInfo.MaxYingLiTimes = updateInfo.CounterYingLi
        }
    }

    // 净利润
    updateInfo.JingLiRun = fInfo.JingLiRun + profit

    // 最大净利润
    max_jing_li_run := math.Max(updateInfo.JingLiRun, fInfo.MaxJingLiRun)
    if max_jing_li_run == 0 {
        updateInfo.MaxJingLiRun = fInfo.MaxJingLiRun
    } else {
        updateInfo.MaxJingLiRun = max_jing_li_run
    }

    // 最大回撤金额
    max_hui_che := max_jing_li_run - updateInfo.JingLiRun
    updateInfo.MaxHuiChePrice = math.Max(fInfo.MaxHuiChePrice, max_hui_che)

    // 交易后的余额，存入每天余额列表
    remain := fInfo.Remaining + profit
//    fmt.Println("remain",fInfo.Remaining, profit, remain, ganggan)
//    updateInfo.Remaining = fInfo.Remaining + profit
    updateInfo.Remaining = remain

    // 最大回撤百分比
    if isProfit == 1 {
        updateInfo.RateMaxHuiChe = (updateInfo.MaxJingLiRun - updateInfo.JingLiRun) / (updateInfo.MaxJingLiRun + fInfo.Capital) * 100
        _, err = Engine.Where("formula_name=? and symbol=?", formula_name, symbol).Cols("last_date, counter_ying_li, counter_kui_sun, max_ying_li_times, max_kui_sun_times, max_jing_li_run, remaining, max_hui_che_price, jing_li_run, rate_max_hui_che").Update(updateInfo)
    } else {

        _, err = Engine.Where("formula_name=? and symbol=?", formula_name, symbol).Cols("last_date, counter_ying_li, counter_kui_sun, max_ying_li_times, max_kui_sun_times, max_jing_li_run, remaining, max_hui_che_price, jing_li_run").Update(updateInfo)
    }
    if err != nil {
        return
    }

    // 保存日期记录
    err = SaveDaliyData(conn, formula_name, symbol, insertDate, remain)
    if err != nil {
        return
    }

    //保存月净利润
    err = SaveMonthProfit(conn, formula_name, symbol, insertDate, remain)
    if err != nil {
        return
    }


    return
}

// 取品种杠杆
func GetSymbolLever(conn redis.Conn, symbol string) (gg int64 , err error) {
    removeNumberRep := regexp.MustCompile(`\d`)
    preV := removeNumberRep.ReplaceAllString(symbol, "")
    return redis.Int64(conn.Do("HGET", "futures.strategy.symbol.lever", preV))
}

// 取策略id
func GetFuturesId(conn redis.Conn,fname, symbol string) (fid string, err error){
    fid, err = redis.String(conn.Do("GET", GetFuturesIdKey(fname, symbol)))
    if err != nil {
        return
    }

    return
}

// 取fid的key
func GetFuturesIdKey(fname, symbol string) string{
    return fmt.Sprintf("futures.strategy.code.to.id.%s%s", fname,symbol)
}

// 取feedID
func GetFeedId(conn redis.Conn, t string) (rid string, err error) {
    // 取id
    ri, err := redis.Int64(conn.Do("INCR", "feed:counter"))
    //ri, err := conn.Do("INCR", "feed:counter")
    if err != nil {
        return
    }

    rid = strconv.FormatInt(ri, 10)

    if rid == "" {
        err = errors.New("数据读取失败")
        return
    }

    return rid+t, nil
}

//是否存在key
func GetRecordExistsKey(fid string) string {
    return fmt.Sprintf("futures:%s:all.record", fid)
}
// 是存已存在redis中
func CheckRecordExistsRedis(conn redis.Conn, fid, recordId string) (has bool, err error) {
    return redis.Bool(conn.Do("SISMEMBER", GetRecordExistsKey(fid), recordId))
}

// 取record hash key
func GetRecordInfoKey(rid string) string{
    return fmt.Sprintf("futures.strategy.result:%s", rid)
}

// 从mysql中同步记录到redis
func Record2Redis(conn redis.Conn, recordId, fname, symbol string) (err error){

    fid, err := GetFuturesId(conn, fname, symbol)
    if err != nil {
        fmt.Println("no key")
        return
    }

    if fid == "" {
        return errors.New("GetFuturesId empty")
    }


    // 是否已存在
    sis, err := CheckRecordExistsRedis(conn, fid, recordId)
    if err != nil {
        return
    }

    // 已存在退出
    if sis {
        // @todo 已存在提示
        // return errors.New("record exists !")
        return
    }

    sql := fmt.Sprintf("select * from tb_record where id=\"%s\"", recordId)
    res, err := Engine.Query(sql)
    if err != nil {
        return
    }

    if len(res) == 0 {
        return errors.New("record info is empty")
    }

    rid, err := GetFeedId(conn, "46")
    if err != nil {
        return
    }
    // 存hash
    key := GetRecordInfoKey(rid)

    args := []interface{}{key}
    args = append(args, "id", rid)
    args = append(args, "futures_id", fid)

    info := make(map[string]string, 0)
    for k,v := range res[0] {
        info[k] = string(v)
        if k == "id" {
            continue
        }

        args = append(args, k, string(v))
    }

    _, err = conn.Do("HMSET", args...)
    if err != nil {
        return
    }

    // 最新id
    //SaveMaxResultId(conn, rid)

    // 存列表
    utime,err := time.Parse("2006-01-02 15:04:05", fmt.Sprintf("%s %s", info["date"], info["time"]))
    if err != nil {
        return
    }

    // daily. futures.strategy.result.by.result.id:[ futures.stragegy.id]:all
    _, err = conn.Do("ZADD", fmt.Sprintf("daily.futures.strategy.result.by.result.id:%s:all", fid), strconv.FormatInt(utime.Unix(), 10), rid)
    if err != nil {
        return
    }

    // daily. futures.strategy.result.by.result.id:[ futures.stragegy.id]:[yyyy-mm-dd]
    _, err = conn.Do("ZADD", fmt.Sprintf("daily.futures.strategy.result.by.result.id:%s:%s", fid, utime.Format("2006-01-02")),strconv.FormatInt(utime.Unix(), 10), rid)
    if err != nil {
        return
    }

    // 存到已存在set
    _, err = conn.Do("SADD", GetRecordExistsKey(fid), recordId)
    if err != nil {
        return
    }

    return
}

// 更新最新记录id
func SaveMaxResultId(conn redis.Conn, rid string) (err error){
    _, err = conn.Do("SET", "futures.strategy.max.result.id", rid)
    return
}

// 保存日期记录
func SaveDaliyData(conn redis.Conn, formula_name, symbol string, date time.Time, remaining float64) (err error){
    fid, err := GetFuturesId(conn, formula_name, symbol)
    if err != nil {
        return
    }

    _, err = conn.Do("ZADD", fmt.Sprintf("futures:%s:daily.data", fid), remaining, date.Unix())
    if err != nil {
        return
    }

    // 月记录
    month := date.Format("2006-01")
    monthTime, err := time.Parse("2006-01", month)
    if err != nil {
        return
    }
    _, err = conn.Do("ZADD", fmt.Sprintf("futures:%s:month.data", fid), remaining, monthTime.Unix())

    return
}

// 保存每月净利润
func SaveMonthProfit(conn redis.Conn, formula_name, symbol string, date time.Time, jingLiRun float64)(err error){
    fid, err := GetFuturesId(conn, formula_name, symbol)
    if err != nil {
        return
    }

    month := date.Format("2006-01")
    monthTime, err := time.Parse("2006-01", month)
    if err != nil {
        return
    }
    _, err = conn.Do("ZADD", fmt.Sprintf("futures:%s:month.netprofit.data", fid), jingLiRun, monthTime.Unix())

    return
}

// 策略infokey
func GetFuturesInfoKey(fid string) string {
    return fmt.Sprintf("futures:%s",fid)
}

// 取MySQL信息info
func GetFuturesMysqlInfo(fname, symbol string) (info map[string]string, err error) {
    sql := fmt.Sprintf("SELECT * FROM `finfo` WHERE formula_name=\"%s\" AND symbol=\"%s\"", fname, symbol)
    res, err := Engine.Query(sql)
    if err != nil {
        return
    }
    if len(res) == 0 {
        err = errors.New("mysql记录不存在")
        return
    }

    info = make(map[string]string, 0)
    for k,v := range res[0]{
        info[k] = string(v)
    }

    return info, nil
}

// 保存info信息到redis
func Save2Redis(conn redis.Conn, fname, symbol string) (err error){

    fid, err := GetFuturesId(conn, fname, symbol)
    if err != nil {
        return
    }

    if fid == "" {
        return
    }

    key := GetFuturesInfoKey(fid)
    args := []interface{}{key}

    info, err := GetFuturesMysqlInfo(fname, symbol)
    if err != nil {
        return
    }
    for k,v := range info {
        if k == "id"{
            continue
        }
        args = append(args, k, v)
    }

    fmt.Println(args)
    fmt.Println(len(args))

    _, err = conn.Do("HMSET", args...)
    if err != nil {
        return
    }

    fmt.Println("Save2Redis ok!")

    return
}

// 更新info信息mysql
func DoUpdateInfo(fname, symbol string) (err error){
    yingliInfo, err := YingliInfo(fname, symbol)
    if err != nil {
        return
    }

    // 总盈利
    sum_ying_li, err := strconv.ParseFloat(yingliInfo["sum_ying_li"], 64)
    if err != nil {
        return
    }

    // 最大盈利
    max_ying_li, err := strconv.ParseFloat(yingliInfo["max_ying_li"], 64)
    if err != nil {
        return
    }

    // 盈利次数
    count_ying_li_times, err := strconv.ParseFloat(yingliInfo["count_ying_li_times"], 64)
    if err != nil {
        return
    }

    // 盈利手数
    count_ying_li_number, err := strconv.ParseFloat(yingliInfo["count_ying_li_number"], 64)
    if err != nil {
        return
    }

    // 平均盈利
    avg_ying_li := 0.0
    if count_ying_li_number > 0 {
        avg_ying_li = sum_ying_li / count_ying_li_number
    }

    kuisunInfo, err := KuiSunInfo(fname, symbol)
    if err != nil {
        return
    }

    // 总亏损
    sum_kui_sun, err := strconv.ParseFloat(kuisunInfo["sum_kui_sun"], 64)
    if err != nil {
        return
    }

    // 最大亏损
    max_kui_sun, err := strconv.ParseFloat(kuisunInfo["max_kui_sun"], 64)
    if err != nil {
        return
    }

    // 亏损手数
    count_kui_sun_number, err := strconv.ParseFloat(kuisunInfo["count_kui_sun_number"], 64)
    if err != nil {
        return
    }

    // 亏损交易次数
    count_kui_sun_times, err := strconv.ParseFloat(kuisunInfo["count_kui_sun_times"], 64)
    if err != nil {
        return
    }
    // 平均亏损
    avg_kui_sun := 0.0
    if count_kui_sun_number > 0 {
        avg_kui_sun = sum_kui_sun / count_kui_sun_number
    }


    // 盈亏比

    rate_ying_kui := 0.0
    if avg_kui_sun != 0 {
        rate_ying_kui = math.Abs(avg_ying_li/avg_kui_sun)
    }

    // 净利润
    jing_li_run := sum_ying_li - math.Abs(sum_kui_sun)

    baseInfo, err := BaseInfo(fname, symbol)
    if err != nil {
        return
    }

    // 本金
    capital, err := strconv.ParseFloat(baseInfo["capital"], 64)
    if err != nil {
        return
    }

    // 最大净利润
    max_jing_li_run, err := strconv.ParseFloat(baseInfo["max_jing_li_run"], 64)
    if err != nil {
        return
    }
    // 取最大净利润

    max_jing_li_run = math.Max(max_jing_li_run, jing_li_run)
    if max_jing_li_run == 0 {
        max_jing_li_run = jing_li_run
    }

    max_hui_che_price, err := strconv.ParseFloat(baseInfo["max_hui_che_price"], 64)
    if err != nil {
        return
    }
    // 最大回撤金额
    //max_hui_che_price = math.Max(max_hui_che_price, (max_jing_li_run - jing_li_run))


    // 净值
    jing_zhi := 0.0
    if capital != 0 {
        jing_zhi = (jing_li_run + capital)/capital * 100
    }

    sumInfo, err := SumInfo(fname, symbol)
    if err != nil {
        return
    }

    // 总交易次数
    count_sell_times, err := strconv.ParseFloat(sumInfo["count_sell_times"], 64)
    if err != nil {
        return
    }

    // 总手数
    sum_number, err := strconv.ParseFloat(sumInfo["sum_number"], 64)
    if err != nil {
        return
    }

    // 胜率
    rate_sheng_lv := 0.0
    if count_sell_times > 0 {
      rate_sheng_lv = count_ying_li_times / count_sell_times * 100
    }


    // 收益率
    rate_shou_yi := 0.0
    if capital != 0 {
        rate_shou_yi = jing_li_run / capital * 100
    }

    // 余额
    remaining := capital + jing_li_run


    oldInfo := new(Finfo)
    has, err := Engine.Where("formula_name=? and symbol=?", fname, symbol).Get(oldInfo)
    if err != nil || !has{
        return
    }


    // 交易天数
    duration := oldInfo.LastDate.Sub(oldInfo.StartDate)
    count_sell_day := math.Ceil(duration.Hours()/24)
    if count_sell_day == 0 {
        count_sell_day = 1
    }
    // 交易月数 进一，不足一月数一月？
    count_sell_months := math.Ceil(count_sell_day / 30.5)

    // 交易年数
    sell_year := oldInfo.LastDate.Year() - oldInfo.StartDate.Year() +1
    fmt.Println("sell_year", sell_year, rate_shou_yi)

    // 月平均收益率
    rate_month_shou_yi := jing_li_run/count_sell_months / capital * 100
    // 月平均收益
    avg_month_shou_yi := jing_li_run / count_sell_day * 3.05
    // 年化收益率
    rate_year_shou_yi := (math.Pow((rate_shou_yi/100+1), 1/float64(sell_year)) -1) * 100
    if rate_shou_yi < 0 {
        rate_year_shou_yi = -rate_year_shou_yi
    }

    /*
    // 最大回撤百分比
    rate_max_hui_che := 0.0
    if max_jing_li_run != 0 {
        rate_max_hui_che = (max_jing_li_run - jing_li_run) / max_jing_li_run
    }
    */

   rate_year_shou_yi_max_hui_che := 0.0
    if oldInfo.RateMaxHuiChe != 0 {
        rate_year_shou_yi_max_hui_che = rate_year_shou_yi / oldInfo.RateMaxHuiChe * 100
    }

    finfo := new(Finfo)
    finfo.Remaining = remaining
    finfo.JingLiRun = jing_li_run
    finfo.SumYingLi = sum_ying_li
    finfo.MaxYingLi = max_ying_li
    finfo.SumKuiSun = sum_kui_sun
    finfo.MaxKuiSun = max_kui_sun
    finfo.CountSellTimes = int64(count_sell_times)
    finfo.CountYingLiTimes = int64(count_ying_li_times)
    finfo.CountKuiSunTimes = int64(count_kui_sun_times)
    finfo.RateShengLv = rate_sheng_lv
    finfo.CountYingLiNumber = int64(count_ying_li_number)
    finfo.CountKuiSunNumber = int64(count_kui_sun_number)
    finfo.SumNumber = int64(sum_number)
    finfo.AvgYingLi = avg_ying_li
    finfo.AvgKuiSun = avg_kui_sun
    finfo.RateYingKui = rate_ying_kui
    finfo.RateShouYi = rate_shou_yi
    finfo.MaxJingLiRun = max_jing_li_run
    finfo.MaxHuiChePrice = max_hui_che_price
    finfo.JingZhi = jing_zhi
    finfo.CountSellDay = int64(count_sell_day)
    finfo.AvgMonthShouYi = avg_month_shou_yi
    finfo.RateYearShouYi = rate_year_shou_yi
    finfo.CountSellMonths = int64(count_sell_months)
    finfo.RateMonthShouYi = rate_month_shou_yi
    finfo.RateYearShouYiMaxHuiChe = rate_year_shou_yi_max_hui_che

    // 夏普指数
    conn := RedisPool.Get()
    defer conn.Close()
    xiapu, err := Xiapu(conn, fname, symbol, capital, rate_month_shou_yi/100, 0.03)
    if err != nil {
        return
    }

    finfo.Xiapu = xiapu*100


    _, err = Engine.Where("formula_name=? and symbol=?", fname, symbol).Update(finfo)
    if err != nil {
        return
    }

    fmt.Println("===== 更新finfo成功  ====")

    /*

    sql :=fmt.Sprintf("UPDATE `info` SET `remaining`=%.6f,`jing_li_run`=%.6f,`sum_ying_li`=%.6f,`max_ying_li`=%.6f,`sum_kui_sun`=%.6f,`max_kui_sun`=%.6f,`count_sell_times`=%f,`count_ying_li_times`=%f,`count_kui_sun_times`=%f,`rate_sheng_lv`=%.2f,`avg_chi_cang_bar`=%f,`count_sell_day`=%f,`avg_month_shou_yi`=%.6f,`count_ying_li_number`=%f,`count_kui_sun_number`=%f,`sum_number`=%f,`avg_ying_li`=%.6f,`avg_kui_sun`=%.6f,`rate_ying_kui`=%.2f,`counter_ying_li`=%f,`counter_kui_sun`=%f,`max_ying_li_times`=%f,`max_kui_sun_times`=%f,`count_sell_months`=%f,`rate_shou_yi`=%.2f,`rate_month_shou_yi`=%.2f,`rate_year_shou_yi`=%.2f,`max_jing_li_run`=%.6f,`max_hui_che_price`=%.6f,`rate_max_hui_che`=%.2f,`rate_year_shou_yi_max_hui_che`=%.2f, `jing_zhi`=%.6f WHERE `formula_name`=\"%s\" and `symbol`=\"%s\"", remaining, jing_li_run, sum_ying_li, max_ying_li, math.Abs(sum_kui_sun), math.Abs(max_kui_sun), count_sell_times, count_ying_li_times, count_kui_sun_times, rate_sheng_lv, 0.0, 0.0, 0.0, count_ying_li_number, count_kui_sun_number, sum_number, avg_ying_li, avg_kui_sun, rate_ying_kui, 0.0, 0.0, 0.0, 0.0, 0.0, rate_shou_yi, 0.0, 0.0, max_jing_li_run, max_hui_che_price, 0.0, 0.0, jing_zhi, fname, symbol)

    _, err = Engine.Exec(sql)
    if err != nil {
        return
    }

    */
    return
}

// 夏普指数
func Xiapu(conn redis.Conn, fname, symbol string, capital, rate_month_shou_yi, rate_yh float64) (xp float64, err error){
    fid, err := GetFuturesId(conn, fname, symbol)
    if err != nil {
        return
    }

    res, err := redis.Strings(conn.Do("ZRANGE", fmt.Sprintf("futures:%s:month.netprofit.data", fid), 0, -1, "withscores"))

    // profits
    profits := make([]float64,0)
    count := 0
    totalProfits := 0.0

    manths := make(map[string]float64, 0)

    for i := 0; i< len(res); i++ {
        if i%2 == 1 {
            f, _ := strconv.ParseFloat(res[i], 64)
            profits = append(profits, f)

            manths[res[i-1]] = f
            count ++
            totalProfits += f
        }
    }

    sortValues := helper.NewMapSorter(manths)

    // 增长率
    upRate := make(map[string]float64, 0)
    // 每月百分比
    lenVals := len(sortValues)
    for i :=0; i<lenVals; i++ {
        v := sortValues[i]
        if i == 0 {
            upRate[v.Key] = v.Val/capital
        }else{
            v2 := sortValues[i-1]
            upRate[v.Key] = (v.Val - v2.Val) / v2.Val
        }
//        fmt.Println(i, v.Key, v.Val)
    }

    totalRate := 0.0
    countRate := 0
    for _, v := range upRate {
//        fmt.Println(k, v)
        totalRate += v
        countRate ++
    }

    avgRate := totalRate/ float64(countRate)

//    fmt.Println(avgRate)


    // 平均值
//    avgProfit := totalProfits/float64(count)

    // 平均值的差
    // 平方和
    powSum := 0.0
    for _,v := range upRate{
        powSum += math.Pow((v - avgRate), 2)
    }
    // 月收益率方差， 
    fx := math.Sqrt(powSum/float64(countRate))

    xp = ((rate_month_shou_yi - rate_yh)/12)/fx

//    fmt.Println(fx, xp)
    
//    fmt.Println(fmt.Sprintf("%.2f, %d, %.2f",totalProfits, count, avgProfit), profits)
//    fmt.Println(fmt.Sprintf("%.2f,%.2f, %.2f",powSum, powSum/float64(count),xp))
    return
}

/**
 * 盈利信息
 * 总盈利 sum_ying_li
 * 最大盈利 max_ying_li
 * 盈利次数 count_yin_li_times
 * 盈利手数 count_yin_li_number
 */
func YingliInfo(fname, symbol string) (list map[string]string, err error){
    sql := fmt.Sprintf("SELECT sum(profit) as sum_ying_li, max(profit) as max_ying_li, sum(number) as count_ying_li_number, count(id) as count_ying_li_times FROM `tb_record` WHERE `formula_name`='%s' and `symbol`='%s' and `is_profit`=3", fname, symbol)
    res, err := Engine.Query(sql)
    if err != nil {
        fmt.Println(err)
        return
    }

    list = make(map[string]string)
    list["sum_ying_li"] = "0"
    list["max_ying_li"] = "0"
    list["count_ying_li_number"] = "0"
    list["count_ying_li_times"] = "0"

    for k,v := range res[0] {
        list[k] = string(v)
    }

    fmt.Println(list)

    return
}

 /**
 * 盈利信息
 * 总盈利 sum_ying_li
 * 最大盈利 max_ying_li
 * 盈利次数 count_yin_li_times
 * 盈利手数 count_yin_li_number
 */
func KuiSunInfo(fname, symbol string) (list map[string]string, err error){
    sql := fmt.Sprintf("SELECT sum(profit) as sum_kui_sun, min(profit) as max_kui_sun, sum(number) as count_kui_sun_number, count(id) as count_kui_sun_times FROM `tb_record` WHERE `formula_name`='%s' and `symbol`='%s' and `is_profit`=1", fname, symbol)
    res, err := Engine.Query(sql)
    if err != nil {
        fmt.Println(err)
        return
    }

    list = make(map[string]string)
    list["sum_kui_sun"] = "0"
    list["max_kui_sun"] = "0"
    list["count_kui_sun_number"] = "0"
    list["count_kui_sun_times"] = "0"

    for k,v := range res[0] {
        list[k] = string(v)
    }

    fmt.Println(list)

    return
}

/**
 *  总信息
 */
func SumInfo(fname, symbol string) (list map[string]string, err error){
    sql := fmt.Sprintf("SELECT sum(number) as sum_number, count(id) as count_sell_times FROM `tb_record` WHERE `formula_name`='%s' and `symbol`='%s' and `is_profit`=1 or `is_profit`=3", fname, symbol)
     res, err := Engine.Query(sql)
    if err != nil {
        fmt.Println(err)
        return
    }

    list = make(map[string]string)
    list["sum_number"] = "0"
    list["count_sell_times"] = "0"

    for k,v := range res[0] {
        list[k] = string(v)
    }

    fmt.Println(list)

    return
}

/**
 *  基本信息
    本金
    资金余额
    开始日期

 */
func BaseInfo(fname, symbol string) (list map[string]string, err error){
    sql := fmt.Sprintf("SELECT `capital`,`remaining`, `start_date`, `max_jing_li_run`, `max_hui_che_price` FROM `finfo` WHERE  `formula_name`='%s' and `symbol`='%s'", fname, symbol)

    fmt.Println("=== sql ===",sql)
    res, err := Engine.Query(sql)
    if err != nil {
        return
    }

    if len(res) == 0 {
        return nil, errors.New("数据不存在")
    }

    list = make(map[string]string)
    for k,v := range res[0] {
        list[k] = string(v)
    }

    fmt.Println(list)

    return
}
