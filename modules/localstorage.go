package modules

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/redis.v3"
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

/* {{{ func (o *OMQ) localStorage(cmd []string) error
 * 处理SET/DEL命令
 */
func (o *OMQ) localStorage(cmd []string) error { //set + del

	if cc == nil && Redis == nil { //没有本地存储
		return fmt.Errorf("can't reach localstorage")
	} else {
		o.Trace("save local storage: %q", cmd)
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
			so := new(StorageOption)
			if err := json.Unmarshal([]byte(option), so); err != nil {
				o.Info("unmarshal option failed: %s", option)
				ls.option = &StorageOption{Type: _STORAGE_REDIS} //默认
			} else {
				ls.option = so
			}
		}
		if len(cmd) >= 4 {
			ls.value = cmd[3]
		}
		o.Trace("[act: %s][key: %s][value: %s]", act, ls.key, ls.value)
		switch act {
		case COMMAND_SET:
			if len(cmd) >= 5 {
				ls.expire, _ = strconv.Atoi(cmd[4])
			}
			o.Trace("expire: %d", ls.expire)
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

/* {{{ func (o *OMQ) localGet(cmd []string) ([]string, error)
 * 处理SET/DEL命令
 */
func (o *OMQ) localGet(cmd []string) (r []string, err error) { //set + del

	if cc == nil && Redis == nil { //没有本地存储
		err = fmt.Errorf("can't reach localstorage")
		return
	} else {
		o.Trace("save local storage: %q", cmd)
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
			so := new(StorageOption)
			if err := json.Unmarshal([]byte(option), so); err != nil {
				o.Info("unmarshal option failed: %s", option)
				ls.option = &StorageOption{Type: _STORAGE_REDIS} //默认
			} else {
				ls.option = so
			}
		}
		o.Trace("[act: %s][key: %s]", act, ls.key)
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

/* {{{ func (ls *LocalStorage) Set() (err error)
 *
 */
func (ls *LocalStorage) Set() (err error) {
	if cc != nil { // use cluster
		err = cc.Set(ls.key, ls.value, time.Duration(ls.expire)*time.Second).Err()
	} else {
		redisConn := Redis.Pool.Get()
		defer redisConn.Close()
		if ls.expire > 0 {
			_, err = redisConn.Do("SET", ls.key, ls.value, "EX", ls.expire)
		} else {
			_, err = redisConn.Do("SET", ls.key, ls.value)
		}
	}
	return
}

/* }}} */

/* {{{ func (ls *LocalStorage) Get() (r []string, err error)
 *
 */
func (ls *LocalStorage) Get() (r []string, err error) {
	if cc != nil { // use cluster
		var rs string
		if rs, err = cc.Get(ls.key).Result(); err != nil {
			return
		} else {
			r = []string{rs}
		}
	} else {
		redisConn := Redis.Pool.Get()
		defer redisConn.Close()
		if result, e := redisConn.Do("GET", ls.key); e == nil {
			if result == nil {
				return nil, ErrNil
			} else {
				if len(result.([]byte)) == 0 {
					return nil, ErrNil
				}
				r = []string{string(result.([]byte))}
			}
		} else {
			err = e
		}
	}
	return
}

/* }}} */

/* {{{ func (ls *LocalStorage) Timing() (r []string, err error)
 *
 */
func (ls *LocalStorage) Timing() (r []string, err error) {
	now := time.Now().Unix() //当前的时间戳
	if cc != nil {           // use cluster
		if rz, e := cc.ZRangeByScoreWithScores(ls.key, redis.ZRangeByScore{Min: "0", Max: strconv.Itoa(int(now))}).Result(); e != nil {
			err = e
		} else {
			r = make([]string, 0)
			for _, rm := range rz {
				r = append(r, rm.Member.(string))
				cc.ZRem(ls.key, rm.Member.(string))
			}
		}
	} else {
		redisConn := Redis.Pool.Get()
		defer redisConn.Close()
		//if result, e := redisConn.Do("ZRANGEBYSCORE", ls.key, 0, now, "WITHSCORES", "LIMIT", 0, 10); e == nil {
		if result, e := redisConn.Do("ZRANGEBYSCORE", ls.key, 0, now, "LIMIT", 0, 10); e == nil {
			if result == nil {
				return nil, ErrNil
			} else {
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
					//zrem := make([]string, 0)
					r = make([]string, 0)
					for k, rtt := range rt {
						v := string(rtt.([]byte))
						r = append(r, v)
						if k%2 == 0 { //偶数为member(0-based)
							//fmt.Printf("k:%d,v:%s\n", k, v)
							//zrem = append(zrem, v)
							redisConn.Do("ZREM", ls.key, v)
						}
					}
					//redisConn.Do("ZREM", ls.key, strings.Join(zrem, " "))
					//redisConn.Do("ZREM", ls.key, zrem)
				default:
					err = fmt.Errorf("unknown type")
				}
			}
		} else {
			err = e
		}
	}
	return
}

/* }}} */

/* {{{ func (ls *LocalStorage) Del() error
 *
 */
func (ls *LocalStorage) Del() (err error) {
	if cc != nil { // use cluster
		err = cc.Del(ls.key).Err()
	} else {
		redisConn := Redis.Pool.Get()
		defer redisConn.Close()
		_, err = redisConn.Do("DEL", ls.key)
	}
	return err
}

/* }}} */

/* {{{ func (ls *LocalStorage) Schedule() (err error)
 *
 */
func (ls *LocalStorage) Schedule() (err error) {
	if cc != nil { // use cluster
		err = cc.ZAdd(ls.key, redis.Z{float64(ls.ts), ls.value}).Err()
	} else {
		redisConn := Redis.Pool.Get()
		defer redisConn.Close()
		_, err = redisConn.Do("ZADD", ls.key, ls.ts, ls.value)
	}
	return
}

/* }}} */
