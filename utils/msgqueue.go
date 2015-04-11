package utils

import (
	"crypto/md5"
	"errors"
	"fmt"
	"time"

	zmq "github.com/pebbe/zmq4"
)

const (
	BLOCK_DURATION = 3 * time.Second //默认阻塞时间
)

type msgqueue struct {
	pusher  *Socket
	queuer  *Socket
	iPoller *zmq.Poller
	oPoller *zmq.Poller
	key     string
	expire  time.Time //最近访问的时间戳
}

type MQPool struct {
	Pool map[string]*msgqueue
	max  int           //最大items数
	life time.Duration //生命周期
}

/* {{{ func NewMQPool() {
 *
 */
func NewMQPool() *MQPool {
	return &MQPool{
		Pool: make(map[string]*msgqueue),
		max:  50,                  //最多50个
		life: 86400 * time.Second, //生命周期10min
	}
}

/* }}} */

/* {{{ func (m *MQPool) Destroy() {
 * 销毁pool
 */
func (m *MQPool) Destroy() {
	if len(m.Pool) > 0 {
		for k, _ := range m.Pool {
			m.remove(k)
		}
	}
}

/* }}} */

/* {{{ func (m *MQPool) Get(key string) (mq *msgqueue, err error) {
 * 获取相关key的队列
 */
func (m *MQPool) Get(key string) (mq *msgqueue, err error) {
	// hash key
	hk := fmt.Sprintf("%x", md5.Sum([]byte(key)))
	now := time.Now()
	expire := now.Add(m.life)
	if _, ok := m.Pool[hk]; ok {
		mq = m.Pool[hk]
	} else {
		if len(m.Pool) >= m.max { //达到最大数,清理
			for k, queue := range m.Pool {
				if now.After(queue.expire) { //过期,死亡
					m.remove(k)
				}
			}
		}
		if len(m.Pool) < m.max {
			mq = &msgqueue{
				pusher:  NewSocket(zmq.DEALER, 1000),
				queuer:  NewSocket(zmq.DEALER, 1000),
				iPoller: zmq.NewPoller(), //in
				oPoller: zmq.NewPoller(), //out
				expire:  expire,
			}
			//建立连接
			mq.pusher.Bind(fmt.Sprint("inproc://", hk))
			mq.queuer.Connect(fmt.Sprint("inproc://", hk))
			mq.oPoller.Add(mq.pusher.soc, zmq.POLLOUT)
			mq.iPoller.Add(mq.queuer.soc, zmq.POLLIN)
			m.Pool[hk] = mq
		} else {
			// pool 满了, 婉拒
			err = fmt.Errorf("pool space full: %d", m.max)
		}
	}
	if err == nil {
		mq.expire = expire
	}
	return
}

/* }}} */

/* {{{ func (m *MQPool) Reach(key string) (mq *msgqueue, err error) {
 * 获取相关key的队列(不新建)
 */
func (m *MQPool) Reach(key string) (mq *msgqueue, err error) {
	// hash key
	hk := fmt.Sprintf("%x", md5.Sum([]byte(key)))
	now := time.Now()
	expire := now.Add(m.life)
	if _, ok := m.Pool[hk]; ok {
		mq = m.Pool[hk]
		mq.expire = expire
	} else {
		err = fmt.Errorf("not found queue: %s", key)
	}
	return
}

/* }}} */

/* {{{ func (m *MQPool) Push(k string, v interface{}) error {
 * 入栈
 */
func (m *MQPool) Push(k string, v interface{}) error {
	if q, err := m.Get(k); err == nil {
		//如果不存在队列,会新建1个
		//sockets, err := q.oPoller.Poll(HEARTBEAT_INTERVAL)
		sockets, err := q.oPoller.Poll(5 * time.Millisecond)
		if err != nil {
			return err
		}
		if len(sockets) == 1 {
			q.pusher.SendMessage(v)
		} else {
			// 发不出去, 说明队列满了
			return fmt.Errorf("REACH HWM!")
		}
	} else {
		return err
	}
	return nil
}

/* }}} */

/* {{{ func (m *MQPool) Pop(k string, ext ...interface{}) (v string, err error)
 * 出栈
 */
func (m *MQPool) Pop(k string, ext ...interface{}) (v []string, err error) {
	if q, err := m.Reach(k); err == nil {
		//不存在队列就不新建了
		//sockets, err := q.iPoller.Poll(HEARTBEAT_INTERVAL)
		//sockets, err := q.iPoller.Poll(5 * time.Millisecond)
		sockets, err := q.iPoller.Poll(BLOCK_DURATION) //阻塞交给omq处理
		if err != nil {
			return nil, err
		}
		if len(sockets) == 1 {
			v, err = q.queuer.RecvMessage(0)
		} else {
			// 收不进来, 说明没有东西
			//return nil, fmt.Errorf("queue %s is empty!", k)
			return nil, errors.New("NIL")
		}
	} else {
		var second bool
		if len(ext) > 0 {
			switch ev := ext[0].(type) {
			case bool:
				second = ev
			}
		}
		if second {
			return nil, errors.New("NIL")
		} else { //第一次访问(正常访问), 需要block
			time.Sleep(BLOCK_DURATION)
			return m.Pop(k, true)
		}
	}
	return
}

/* }}} */

/* {{{ func (m *MQPool) remove(key string) error {
 * 删除
 */
func (m *MQPool) remove(key string) {
	m.Pool[key].pusher.Close()
	m.Pool[key].queuer.Close()
	delete(m.Pool, key)
}

/* }}} */
