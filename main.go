package main

import(
    "github.com/weisd/tbzb/base"
    "github.com/howeyc/fsnotify"
    "errors"
    "path"
    "path/filepath"
    "os"
    "time"
    "strings"
    "bufio"
    "io"
    "regexp"
    "strconv"
    "fmt"
    M "github.com/weisd/tbzb/models"
)

var(
    WatchPath string
    eventTime   = make(map[string]int64)
    buildPeriod time.Time
    leverMap map[string]int64   // 杠杆
)

func init(){
    // 初始化配置，log
    base.GlobalInit("conf.ini")
    M.RedisInit()
    M.NewXorm()

    // 预存杠杆
    prestoreLever()
}

func main(){
   base.Info("start ...")
    WatchPath = base.Cfg.MustValue("base", "path", "") 
    // 第一次运行先读一次
    err := showDir(WatchPath)
    if err != nil {
        base.Error(err)
    }
    // 启用监控
    p, err := filepath.Abs(WatchPath)
    if err != nil {
        panic(err)
    }
    base.Info(p)
    if WatchPath == "" {
        base.Error("配置监控路径没写")
        panic(errors.New("conf.ini base-path not found"))
    }
    startWatch(p)
}

// 启用监控文件夹
func startWatch(watchPath string){
    base.Info("监控目录:", watchPath)
    watcher, err := fsnotify.NewWatcher()
    if err != nil {
        base.Error("NewWatcher 失败, 程序退出")
        panic(err)
    }

    done := make(chan bool)

    go func(){
        for{
            select{
            case e := <-watcher.Event:
                base.Info("监控事件", e)
                // checkfile 不是log文件不处理
                if !checkFile(e.Name){
                    continue
                }
                if e.IsDelete(){
                    continue
                }
				if buildPeriod.Add(1 * time.Second).After(time.Now()) {
					continue
				}
				buildPeriod = time.Now()

				mt := getFileModTime(e.Name)
				if t := eventTime[e.Name]; mt == t {
                    continue
				}
                eventTime[e.Name] = mt

                //
                readFile(e.Name)

            case err := <-watcher.Error:
                // log
                base.Error("监控事件错误信息：", err)
            }
        }
    }()

    err = watcher.Watch(watchPath)
    if err != nil {
        panic(err)
    }

    <-done

    watcher.Close()
}


// 读文件
func readFile(fileName string){
    // 取文件信息
    _, symbol, _, err  := GetFileNameInfo(fileName);
    // 通过文件名判断
    // 计算利润杠杆
    lever, err := GetLever(GetLeverPrev(symbol))
    if err != nil {
        base.Error("对应杠杆不存在:", symbol)
        return
    }

    f, err := os.Open(fileName)
    if err != nil {
        base.Error("文件读取失败", err)
        return
    }
    defer f.Close()

    bufReader := bufio.NewReader(f)

    L: for{
        line, err := bufReader.ReadString('\n')
        base.Info(line, err)
        if err != nil || err == io.EOF {
            break
        }

        line = strings.Replace(line, "\r\n", "", -1)

        keyVals := strings.Split(line, ";")
        // 格式不对
        kvsCount := len(keyVals)
        if kvsCount < 8 {
            continue
        }

        info := make(map[string]string, 0)
        for i := 0 ; i<kvsCount; i++ {
            base.Info(keyVals[i])
            kvArr := strings.Split(keyVals[i], "=")
            key := kvArr[0]
            val := kvArr[1]
            // 转换
            if key == "date" {
                dateTime, err := time.Parse("20060102", val)
                if err != nil {
                    base.Error("记录时间格式不对", err)
                    break L
                }
                val = dateTime.Format("2006-01-02")
            } else if key == "time" {
                f64, err := strconv.ParseFloat(val, 64);
                if err != nil {
                    base.Error("记录时间格式不对", err)
                    break L
                }
                timeTime, err := time.Parse("0.150405", fmt.Sprintf("%0.6f", f64))
                if err != nil {
                    base.Error("记录时间格式不对", err)
                    break L
                }
                val = timeTime.Format("15:04:05")
            } else if key == "Symbol" {
                info["SymbolPre"] = GetLeverPrev(val)
            }

            info[key] = val
        }

        info["Lever"] = strconv.Itoa(int(lever))

        // 保存到mysql
        rid, err := M.SaveZhiboRecord(info)
        if err != nil {
            base.Error("记录写入mysql失败", err)
            continue
        }

        err = M.Mysql2Redis(rid)
        if err != nil {
            base.Error("记录同步到redis失败", err)
            continue
        }

        base.Trace("记录写入mysql ok", rid, info)
    }

}

// 遍历文件夹
func showDir(pathStr string) error{
    filepath.Walk(pathStr, func(p string, f os.FileInfo, err error) error{
        if f == nil {
            return err
        }
    
        if !checkFile(p){
            return errors.New("不是日志文件")
        }
    
        if f.IsDir(){
            base.Info("过滤目录", p)
            return nil
        }
    
        readFile(p)
    
        return nil 
    })
}

// 不是log文件不处理
func checkFile(fileName string) bool {
    return strings.HasSuffix(strings.ToLower(fileName), "txt")
}

// getFileModTime retuens unix timestamp of `os.File.ModTime` by given path.
func getFileModTime(path string) int64 {
	path = strings.Replace(path, "\\", "/", -1)
	f, err := os.Open(path)
	if err != nil {
		return time.Now().Unix()
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return time.Now().Unix()
	}

	return fi.ModTime().Unix()
}

// 取杠杆
func GetLever(symbol_pre string) (lever int64, err error){
    lever, ok := leverMap[symbol_pre];
    if ok {
        return 
    }

    conn := M.RedisPool.Get()
    defer conn.Close()

    lever, err = M.GetLever(conn, symbol_pre)
    if err != nil {
        return
    }

    leverMap[symbol_pre] = lever

    return
}
// 预存杠杆数据
func prestoreLever(){
    conn := M.RedisPool.Get()
    defer conn.Close()


    
}

// 取品种前缀
func GetLeverPrev(symbol string) string {
    removeNumberRep := regexp.MustCompile(`\d`)
    return removeNumberRep.ReplaceAllString(symbol, "")
}

// 取文件名信息
func GetFileNameInfo(fileName string) (fname, symbol, date string, err error) {
    // 
    isTxt := strings.HasSuffix(strings.ToLower(fileName), "txt")
    if !isTxt {
        err = errors.New("文件不是日志文件")
        return
    }

    _, file := path.Split(filepath.ToSlash(fileName))
    names := strings.Split(path.Base(file), "#")
    if len(names) < 3 {
        err = errors.New("方格名格式不正确")
        return
    }

    fname = strings.TrimLeft(names[0], "$")
    symbol = names[1]
    date = names[2]

    return
}
