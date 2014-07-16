package models

import(
   _ "github.com/go-sql-driver/mysql"
   "github.com/go-xorm/xorm"
    "fmt"
    "github.com/weisd/tbzb/base"
    "time"
    "errors"
    "strconv"
)

var(
    Engine *xorm.Engine
)

// mysql engine
func NewXorm(){
    user := base.Cfg.MustValue("mysql", "user")
    pass := base.Cfg.MustValue("mysql", "password")
    host := base.Cfg.MustValue("mysql", "host", "localhost")
    dbname := base.Cfg.MustValue("mysql", "dbname")
    port := base.Cfg.MustValue("mysql", "port", "3306")
    charset := base.Cfg.MustValue("mysql", "charset", "utf8")

    dns := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s&parseTime=true", user, pass, host, port, dbname, charset)

    var err error
    Engine, err = xorm.NewEngine("mysql", dns)
    if err != nil {
        base.Error("数据库连接失败")
        panic(err)
    }

    Engine.ShowSQL = true

    base.Info("mysql connect ok !", Engine)

}

// 
type ZhiboRecord struct {
    Id int64
    FormulaName string
    SymbolPre   string
    Symbol      string
    Time        time.Time
    Action      string
    Number      int64
    Price       float64
    EntryPrice  float64
    NowPosition int64
    BarNum      int64
    Profit      float64
    IsProfit    bool
    Lever       int64
    AddTime     time.Time
}

// 保存直播记录到mysql")
func SaveZhiboRecord(info map[string]string) (rid int64, err error) {
    base.Info("保存直播记录到mysql")
    base.Info(info)

    formula_name, ok := info["FormulaName"]
    if !ok {
        err = errors.New("FormulaName不存在")
        return
    }

    symbol_pre, ok := info["SymbolPre"]
    if !ok {
        err = errors.New("SymbolPre不存在")
        return
    }

    symbol, ok := info["Symbol"]
    if !ok {
        err = errors.New("Symbol不存在")
        return
    }

    dateStr, ok := info["date"]
    if !ok {
        err = errors.New("date不存在")
        return
    }

    timeStr, ok := info["time"]
    if !ok {
        err = errors.New("time不存在")
        return
    }

    action, ok := info["action"]
    if !ok {
        err = errors.New("action不存在")
        return
    }

    number, ok := info["number"]
    if !ok {
        err = errors.New("number不存在")
        return
    }

    price, ok := info["price"]
    if !ok {
        err = errors.New("price不存在")
        return
    }

    entry_price, ok := info["EntryPrice"]
    if !ok {
        err = errors.New("EntryPrice, 不存在")
        return
    }

    bar_num, ok := info["BarNum"]
    if !ok {
        err = errors.New("BarNum不存在")
        return
    }

    now_position, ok := info["nowPosition"]
    if !ok {
        err = errors.New("nowPosition不存在")
        return
    }

    
    leverStr, ok := info["Lever"]
    if !ok {
        err = errors.New("Lever 不存在")
        return
    }


    //类型转换
    datetimeStr := fmt.Sprintf("%s %s", dateStr, timeStr)

    // 判断 是否已经存在
    recordExists := new(ZhiboRecord)

    /*
    has, err := Engine.Where("formula_name=? and symbol_pre=? and time=? and action=?", formula_name, symbol_pre, datetimeStr, action).Get(recordExists)
    if has {
        base.Warn("记录已经存在", err)
        return nil
    }
    */

    loc, _ := time.LoadLocation("Asia/ShangHai")
    dateTime , err := time.ParseInLocation("2006-01-02 15:04:05", datetimeStr, loc)
    if err != nil {
        return
    }
  

    recordExists.FormulaName = formula_name
    recordExists.SymbolPre = symbol_pre
    recordExists.Action = action
    recordExists.Time = dateTime

    has, err := Engine.Get(recordExists)
    if has {
        base.Warn("记录已经存在", err)
        return recordExists.Id, nil 
    }

    priceFloat, err := strconv.ParseFloat(price, 64)
    if err != nil {
        return 
    }

    entryPriceFloat, err := strconv.ParseFloat(entry_price, 64)
    if err != nil {
        return
    }

    numberInt, err := strconv.ParseInt(number, 10, 64)
    if err != nil {
        return
    }

    barNumberInt, err := strconv.ParseInt(bar_num, 10, 64)
    if err != nil {
        return
    }

    positionInt, err := strconv.ParseInt(now_position, 10, 64)
    if err != nil {
        return
    }

    lever, err := strconv.ParseInt(leverStr, 10, 64)
    if err != nil {
        return
    }

    // 计算
    profit := 0.0
    if action == "sell" {
        profit = (priceFloat - entryPriceFloat) * float64(numberInt) * float64(lever)
    } else if action == "buytocover" {
        profit = (entryPriceFloat - priceFloat) * float64(numberInt) * float64(lever)
    }

    isProfit := false
    if profit > 0 {
        isProfit = true
    }

    base.Warn(dateTime)
    base.Warn(time.Now())
    record := new(ZhiboRecord)
    record.FormulaName = formula_name
    record.SymbolPre   = symbol_pre
    record.Symbol      = symbol
    record.Action      = action
    record.Time        = dateTime
    record.Price       = priceFloat
    record.EntryPrice  = entryPriceFloat
    record.Number      = numberInt
    record.NowPosition = positionInt
    record.AddTime     = time.Now()
    record.BarNum      = barNumberInt
    record.Profit      = profit
    record.IsProfit    = isProfit
    record.Lever       = lever

    _, err = Engine.Insert(record)
    if err != nil {
        return
    }

    return
}
