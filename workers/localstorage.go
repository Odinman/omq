package workers

import (
	"fmt"
	"strconv"
	"strings"
)

type LocalStorage struct {
	storage string // 存储类型, redis
	key     string
	value   string
	expire  int
}

func (w *OmqWorker) localStorage(cmd []string) error {

	w.Trace("save local storage: %q", cmd)

	// 解析命令
	if len(cmd) >= 3 {
		act := strings.ToLower(cmd[0])
		ls := new(LocalStorage)
		ls.key = cmd[1]
		ls.value = cmd[2]
		if strings.ToLower(ls.key) == "redis" && len(cmd) >= 4 {
			//兼容旧版,同时意味着redis当有点不安全
			ls.key = cmd[2]
			ls.value = cmd[3]
		} else {
			if len(cmd) >= 4 {
				ls.expire, _ = strconv.Atoi(cmd[3])
			}
		}
		w.Info("[act: %s][key: %s][value: %s][expire: %d]", act, ls.key, ls.value, ls.expire)
		switch act {
		case "set":
			return ls.Set()
		case "get":
			return ls.Get()
		case "del":
			return ls.Del()
		default:
			return fmt.Errorf("action error: %s", act)
		}
	} else {
		return fmt.Errorf("command error: %s", cmd)
	}

	return nil
}

func (ls *LocalStorage) Set() (err error) {
	redisConn := Redis.Pool.Get()
	defer redisConn.Close()
	if ls.expire > 0 {
		_, err = redisConn.Do("SET", ls.key, ls.value, "EX", ls.expire)
	} else {
		_, err = redisConn.Do("SET", ls.key, ls.value)
	}
	return
}

func (ls *LocalStorage) Get() error {
	return nil
}
func (ls *LocalStorage) Del() error {
	redisConn := Redis.Pool.Get()
	defer redisConn.Close()
	_, err := redisConn.Do("DEL", ls.key)
	return err
}
