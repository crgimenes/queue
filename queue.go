package queue

import (
	"errors"
	"time"

	"github.com/nuveo/beanstalk"
	"github.com/nuveo/log"
)

type bsf struct {
	Conn func(tube string) (c *beanstalk.Conn, err error)
}

var bs bsf

func init() {
	bs.Conn = Conn
}

// Conn return a pointer to beanstalk or error
func Conn(tube string) (c *beanstalk.Conn, err error) {
	c, err = beanstalk.Dial("tcp", tube)
	return
}

// ConnectLoop try to connect beanstalk and retry if error
func ConnectLoop(tube string, retries int) (conn *beanstalk.Conn, err error) {
	attempt := 0
	for {
		conn, err = bs.Conn(tube)
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
