package main

import(
    "fmt"
    "os"
    "path"
    "path/filepath"
    "bufio"
    "io"
    "time"
    "strings"
    "runtime"
	"github.com/astaxie/beego/logs"
    "github.com/howeyc/fsnotify"
    M "github.com/weisd/tblog/models"
)

var (
eventTime   = make(map[string]int64)
buildPeriod time.Time
DirPath string
)

var Loger *logs.BeeLogger

func init(){
    Loger = logs.NewLogger(10000)
    err := Loger.SetLogger("console", "")
    if err != nil {
        fmt.Println("can not init console log", err)
    }

    DirPath = M.Cfg.MustValue("base", "path")
}

func main(){
   exit := make(chan bool)
   Loger.Info("start ...")

   /*
   conn := M.RedisPool.Get()
   defer conn.Close()

   M.Xiapu(conn, "a_aRbreak_6w", "IF888", 600000, 0.3, 0.03)

   return
   */
    initData(DirPath)
    NewWatcher(DirPath)

    for {
      select{
        case <-exit:
            runtime.Goexit()
      }
    }

}

// 每次启动时，读一偏文件，入库
func initData(dirPath string){
    filepath.Walk(dirPath, func(path string, f os.FileInfo, err error) error {
        if f == nil {
            return err
        }

        if f.IsDir(){
            return nil
        }

        //path = strings.ToLower(path)
        if !strings.HasSuffix(path, "TXT"){
            return nil
        }
        //fmt.Println(path)

        Save2Mysql(path)

        return nil
    })
}

// 监控目录
func NewWatcher(pathstr string){
    watcher, err := fsnotify.NewWatcher()
    if err != nil {
        fmt.Println("[ERRO] NewWatcher Failed")
        os.Exit(2)
    }


    go func(){
        for {
            select{
              case e := <-watcher.Event:
                  if e.IsDelete() {
                      continue
                  }

                  // Prevent duplicated builds.
				if buildPeriod.Add(1 * time.Second).After(time.Now()) {
					continue
				}
				buildPeriod = time.Now()

                  _, file := path.Split(filepath.ToSlash(e.Name))
                  fnames := strings.Split(path.Base(file), "#")

                  if len(fnames) <3 {
                      Flog("[INFO]:非日志文件", file)
                      continue
                  }


				mt := getFileModTime(e.Name)
				if t := eventTime[e.Name]; mt == t {
					Flog("[SKIP] # %s #\n", e.String())
                    continue
				}
                eventTime[e.Name] = mt
                
                Save2Mysql(e.Name)
              case err := <-watcher.Error:
                  Flog("err: %s", err.Error())
            }
        }
    }()

    err = watcher.Watch(pathstr)
    if err != nil {
        Flog("err : fail to watch dir ", err)
        os.Exit(2)
    }


}

func Flog(msg string, args... interface{}){
    fmt.Println(msg, args)
}

func Save2Mysql(file string){

    /*
    if !strings.HasSuffix(file, "TXT"){
            return 
    }
    */

    f, err := os.Open(file)
    if err != nil {
        Flog("[ERRO] 文件读取失败:", err.Error())
        return
    }
    defer f.Close()

    Flog("[INFO]:读取文件：", file)
    _, file = path.Split(filepath.ToSlash(file))
    fnames := strings.Split(path.Base(file), "#")

    if len(fnames) <3 {
        Flog("[INFO]:非日志文件", file)
        return
    }

    // 判断mysql，redis info是否已存在
    // 判断 对应杠杆是否已存在
    fname := strings.TrimLeft(fnames[0], "$")
    symbol := fnames[1]
    fmt.Println(fname, symbol)
    bool_isMysqlInfo := M.CheckMysqlInfoExists(fname, symbol)

    if !bool_isMysqlInfo {
        fmt.Println("mysql info 不存在")
        return
    }

    conn := M.RedisPool.Get()
    defer conn.Close()
    // 取redis里的fid
    _, err = M.GetFuturesId(conn, fname, symbol)
    if err != nil {
        fmt.Println("redis 里没有对就的id no key")
        return
    }

    _, err = M.GetSymbolLever(conn, symbol)
    if err != nil {
        fmt.Println(fmt.Sprintf("没有对应的品种杠杆:%s", symbol))
        return
    }


    bufreader := bufio.NewReader(f)

    count := 0
    for{
        line, err := bufreader.ReadString('\n')

        if err == io.EOF{
            break
        }

        line = strings.Replace(line, "\r\n", "", -1)

        info := make(map[string]string)

        kvs := strings.Split(line, ";")
        if len(kvs) < 5 {
            continue
        }
        for i := 0; i< len(kvs); i++{
            kv := strings.Split(kvs[i], "=")
            if len(kv) != 2 {
                Flog("[ERRO] 行中键值对格式不正确")
                continue
            }

            info[kv[0]] = kv[1]
        }

        err = M.SaveTbRecord(info)
        if err != nil {
            Flog("[ERRO] 写入数据库失败", err, info)
            continue 
        }


        /*
        err = M.DoUpdateInfo(sname, symbol)
    if err != nil {
        Flog("[ERRO]:update info failed",err)
        continue
    }

    err = M.Save2Redis(sname, symbol)
    if err != nil {
        Flog("[ERRO]:save2redis failed!", err)
        continue
    }
    */



        // 保存成功
        count ++
    }

    // 没有添加记录
    if count == 0 {
        Flog("[INFO]:没有新记录被添加")
        return
    }

    Flog("[INFO]: 共写入数据条数：", count)
    // 存完 record 再计算stats
    // 从文件名中得到策略名称
    /*
    Flog("[INFO]:读取文件：", file)
    _, file = path.Split(filepath.ToSlash(file))
    fnames := strings.Split(path.Base(file), "#")
    if len(fnames) <3 {
        return
    }
    */
    
    // 更新统计信息
    err = M.DoUpdateInfo(fname, fnames[1])
    if err != nil {
        Flog("[ERRO]:update info failed",err)
        return
    }

    // 更新info到redis
    err = M.Save2Redis(conn, fname, fnames[1])
    if err != nil {
        Flog("[ERRO]:save2redis failed!", err)
        return
    }

    return
}

// getFileModTime retuens unix timestamp of `os.File.ModTime` by given path.
func getFileModTime(path string) int64 {
	path = strings.Replace(path, "\\", "/", -1)
	f, err := os.Open(path)
	if err != nil {
		Flog("[ERRO] Fail to open file[ %s ]\n", err)
		return time.Now().Unix()
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		Flog("[ERRO] Fail to get file information[ %s ]\n", err)
		return time.Now().Unix()
	}

	return fi.ModTime().Unix()
}

