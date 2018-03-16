package queue

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/nuveo/beanstalk"
	"github.com/nuveo/log"
)

type bsf struct {
	Conn func(tube string) (c *beanstalk.Conn, err error)
}

var (
	bs              bsf
	errThereIsNoJob = errors.New("there is no job to be reserved")
)

func init() {
	bs.Conn = Conn
}

// Conn return a pointer to beanstalk or error
func Conn(addr string) (c *beanstalk.Conn, err error) {
	c, err = beanstalk.Dial("tcp", addr)
	return
}

// ConnectLoop try to connect beanstalk and retry if error
func ConnectLoop(addr string, retries int) (conn *beanstalk.Conn, err error) {
	attempt := 0
	for {
		conn, err = bs.Conn(addr)
		if err == nil {
			return
		}
		log.Errorln(err)
		if attempt > retries {
			err = errors.New("exceeded number of connection retries")
			return
		}
		attempt++
		<-time.After(2 * time.Second)
	}
}

// interactWithQueue get for valid jobs in queue and send to handler function
func interactWithQueue(tubeSet beanstalk.TSI, handler func(payload []byte) (err error)) (err error) {
	id, body, err := tubeSet.Reserve(5 * time.Second)
	if err != nil {
		cerr, ok := err.(beanstalk.ConnError)
		if ok && cerr.Err == beanstalk.ErrTimeout && id == 0 {
			// there is no job to be reserved
			err = errThereIsNoJob
			return
		}
		log.Errorln(err)
		return
	}

	if id == 0 || len(body) == 0 {
		err = fmt.Errorf("zero id %v", string(body))
		return
	}

	err = handler(body)
	if err != nil {
		return
	}
	err = tubeSet.Delete(id)
	return
}

func closer(body io.Closer) {
	err := body.Close()
	if err != nil {
		log.Errorln(err)
	}
}

// Listen connect to queue server and wait for messages from the queue
func Listen(addr, tube string, handler func(payload []byte) (err error)) {
	conn, err := ConnectLoop(addr, 500)
	if err != nil {
		log.Errorln(err)
		return
	}
	defer closer(conn)

	ts := beanstalk.NewTubeSet(conn, tube)
	for {
		<-time.After(time.Duration(2) * time.Second)
		err = interactWithQueue(ts, handler)
		if err != nil {
			if err == errThereIsNoJob {
				continue
			}
			log.Errorln(err)
		}
	}
}
