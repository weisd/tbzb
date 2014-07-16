package base

import(
    "github.com/Unknwon/goconfig"
)

var (
    Cfg *goconfig.ConfigFile
)

func GlobalInit(configFile string){
    NewConfig(configFile)

    logMode := Cfg.MustValue("debug", "mode", "console")
    logConfig := Cfg.MustValue("debug", "config", "")

    NewLogger(logMode, logConfig)

    
}

func NewConfig(configFile string){
    var err error
    Cfg, err = goconfig.LoadConfigFile(configFile)
    if err != nil {
        panic(err)
    }
}
