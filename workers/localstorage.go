package workers

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const (
	_STORAGE_REDIS  = "redis"
	_STORAGE_MYSQL  = "mysql"
	_STORAGE_ORACLE = "oracle"
)

type StorageOption struct {
	Type  string
	Db    string
	Table string //当Type为mysql/oracle有用
	Host  string
	Port  string
	Pwd   string
}

type LocalStorage struct {
	option *StorageOption // 存储选项
	key    string
	value  string
	expire int
}

/* {{{ func (w *OmqWorker) localStorage(cmd []string) error
 * 处理SET/DEL命令
 */
func (w *OmqWorker) localStorage(cmd []string) error { //set + del

	if Redis == nil { //没有本地存储
		return fmt.Errorf("can't reach localstorage")
	} else {
		w.Trace("save local storage: %q", cmd)
	}

	// 解析命令
	if len(cmd) >= 4 {
		act := strings.ToLower(cmd[0])
		ls := new(LocalStorage)
		option := cmd[1]
		ls.key = cmd[2]
		if option == "" || strings.ToLower(option) == "redis" {
			//兼容旧版, 新版应该传入一个json,或者为空
			ls.option = &StorageOption{Type: _STORAGE_REDIS}
		} else {
			//解析
			o := new(StorageOption)
			if err := json.Unmarshal([]byte(option), o); err != nil {
				w.Info("unmarshal option failed: %s", option)
				ls.option = &StorageOption{Type: _STORAGE_REDIS} //默认
			} else {
				ls.option = o
			}
		}
		ls.value = cmd[3]
		if len(cmd) >= 5 {
			ls.expire, _ = strconv.Atoi(cmd[3])
		}
		w.Info("[act: %s][key: %s][value: %s][expire: %d]", act, ls.key, ls.value, ls.expire)
		switch act {
		case "set":
			return ls.Set()
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

/* }}} */

/* {{{ func (w *OmqWorker) localGet(cmd []string) (string, error)
 * 处理SET/DEL命令
 */
func (w *OmqWorker) localGet(cmd []string) (r string, err error) { //set + del

	if Redis == nil { //没有本地存储
		err = fmt.Errorf("can't reach localstorage")
		return
	} else {
		w.Trace("save local storage: %q", cmd)
	}

	// 解析命令
	if len(cmd) >= 3 {
		act := strings.ToLower(cmd[0])
		ls := new(LocalStorage)
		option := cmd[1]
		ls.key = cmd[2]
		if option == "" || strings.ToLower(option) == "redis" {
			//兼容旧版, 新版应该传入一个json,或者为空
			ls.option = &StorageOption{Type: _STORAGE_REDIS}
		} else {
			//解析
			o := new(StorageOption)
			if err := json.Unmarshal([]byte(option), o); err != nil {
				w.Info("unmarshal option failed: %s", option)
				ls.option = &StorageOption{Type: _STORAGE_REDIS} //默认
			} else {
				ls.option = o
			}
		}
		w.Info("[act: %s][key: %s]", act, ls.key)
		return ls.Get()
	} else {
		err = fmt.Errorf("command error: %s", cmd)
	}

	return
}

/* }}} */

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

func (ls *LocalStorage) Get() (r string, err error) {
	redisConn := Redis.Pool.Get()
	defer redisConn.Close()
	if result, e := redisConn.Do("GET", ls.key); e == nil {
		r = string(result.([]byte))
	} else {
		err = e
	}
	return
}

func (ls *LocalStorage) Del() error {
	redisConn := Redis.Pool.Get()
	defer redisConn.Close()
	_, err := redisConn.Do("DEL", ls.key)
	return err
}
