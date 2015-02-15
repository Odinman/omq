package workers

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
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
	ts     int
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
		act := strings.ToUpper(cmd[0])
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
		w.Info("[act: %s][key: %s][value: %s][expire: %d]", act, ls.key, ls.value, ls.expire)
		switch act {
		case COMMAND_SET:
			if len(cmd) >= 5 {
				ls.expire, _ = strconv.Atoi(cmd[4])
			}
			return ls.Set()
		case COMMAND_DEL:
			return ls.Del()
		case COMMAND_SCHEDULE:
			if len(cmd) >= 5 {
				ls.ts, _ = strconv.Atoi(cmd[4])
			}
			return ls.Schedule()
		default:
			return fmt.Errorf("action error: %s", act)
		}
	} else {
		return fmt.Errorf("command error: %s", cmd)
	}

	return nil
}

/* }}} */

/* {{{ func (w *OmqWorker) localGet(cmd []string) ([]string, error)
 * 处理SET/DEL命令
 */
func (w *OmqWorker) localGet(cmd []string) (r []string, err error) { //set + del

	if Redis == nil { //没有本地存储
		err = fmt.Errorf("can't reach localstorage")
		return
	} else {
		w.Trace("save local storage: %q", cmd)
	}

	// 解析命令
	if len(cmd) >= 3 {
		act := strings.ToUpper(cmd[0])
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
		switch act {
		case COMMAND_GET:
			return ls.Get()
		case COMMAND_TIMING:
			return ls.Timing()
		default:
			return nil, fmt.Errorf("action error: %s", act)
		}
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

func (ls *LocalStorage) Get() (r []string, err error) {
	redisConn := Redis.Pool.Get()
	defer redisConn.Close()
	if result, e := redisConn.Do("GET", ls.key); e == nil {
		if result == nil {
			r = nil
		} else {
			r = []string{string(result.([]byte))}
		}
	} else {
		err = e
	}
	return
}

func (ls *LocalStorage) Timing() (r []string, err error) {
	redisConn := Redis.Pool.Get()
	defer redisConn.Close()
	now := time.Now().Unix() //当前的时间戳
	//if result, e := redisConn.Do("ZRANGEBYSCORE", ls.key, 0, now, "LIMIT", 0, 1); e == nil {
	if result, e := redisConn.Do("ZRANGEBYSCORE", ls.key, 0, now, "WITHSCORES", "LIMIT", 0, 10); e == nil {
		if result != nil {
			switch rt := result.(type) {
			case []byte:
				if len(rt) == 0 {
					return nil, ErrNil
				}
				r = []string{string(rt)}
			case []interface{}:
				if len(rt) == 0 {
					return nil, ErrNil
				}
				r = make([]string, 0)
				for _, rtt := range rt {
					r = append(r, string(rtt.([]byte)))
				}
			default:
				err = fmt.Errorf("unknown type")
			}
		}
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

func (ls *LocalStorage) Schedule() (err error) {
	redisConn := Redis.Pool.Get()
	defer redisConn.Close()
	_, err = redisConn.Do("ZADD", ls.key, ls.ts, ls.value)
	return
}
