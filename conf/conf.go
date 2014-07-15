package conf

import(
    "strings"
    "path"
    "path/filepath"
    "os"
    "os/exec"
    "github.com/Unknwon/goconfig"
)

var WorkDir string

func init(){
    var err error
    WorkDir, err = ExecDir()
    if err != nil {
        panic(err)
    }
}

// 实例配置
func NewCfg(file string) (cfg *goconfig.ConfigFile, err error) {
    cfgPath := filepath.Join(WorkDir, file)
    cfg, err = goconfig.LoadConfigFile(cfgPath)
    if err != nil {
        return
    }

    return
}

// 执行文件所在目录
func ExecDir() (string, error) {
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		return "", err
	}
	p, err := filepath.Abs(file)
	if err != nil {
		return "", err
	}
	return path.Dir(strings.Replace(p, "\\", "/", -1)), nil
}


