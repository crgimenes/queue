package queue

import (
	"encoding/json"
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

type messagePayload struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
}

func closer(body io.Closer) {
	err := body.Close()
	if err != nil {
		log.Errorln(err)
	}
}

func Loop(conn *beanstalk.Conn) {
	defer closer(conn)
	ts := beanstalk.NewTubeSet(conn, conn.Tube.Name)
	defer closer(ts.Conn)

	for {
		<-time.After(time.Duration(2) * time.Second)
		id, body, err := ts.Conn.PeekReady()
		if err != nil {
			continue
		}
		// if the ID is 0, continue (do not know why the ID is coming empty)
		// or if body is empty, continue
		if id == 0 || len(body) == 0 {
			continue
		}

		msg := messagePayload{}

		err = json.Unmarshal(body, &msg)
		if err != nil {
			log.Println(err)
			err = ts.Conn.Delete(id)
			if err != nil {
				log.Errorln(err)
			}
			continue
		}

		id, body, err = ts.Reserve(5 * time.Second)
		if cerr, ok := err.(beanstalk.ConnError); ok &&
			cerr.Err == beanstalk.ErrTimeout &&
			id != 0 {
			log.Errorf("beanstalk id: %v, err: %v, body: %q\n", id, cerr, string(body))
			err = ts.Conn.Delete(id)
			if err != nil {
				log.Errorln(err)
			}
			continue
		}

		fmt.Println("beanstalk id: %v, body: %q\n", id, string(body))
	}
}
