package main

import (
	"fmt"
	"io"
	"time"

	"github.com/go-br/queue"
	"github.com/nuveo/beanstalk"
	"github.com/nuveo/log"
)

func closer(body io.Closer) {
	err := body.Close()
	if err != nil {
		log.Errorln(err)
	}
}

func main() {
	conn, err := queue.ConnectLoop("localhost:11300", 200)
	if err != nil {
		log.Fatal(err)
	}
	defer closer(conn)

	conn.Tube.Name = "test_queue"
	ts := beanstalk.NewTubeSet(conn, conn.Tube.Name)

	//tube := &beanstalk.Tube{conn, "mytube"}
	id, err := ts.Conn.Put([]byte(`payload test`), 1, 0, 120*time.Minute)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("job id:", id)
}
