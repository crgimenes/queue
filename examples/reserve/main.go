package main

import (
	"fmt"

	"github.com/go-br/queue"
)

func handler(payload []byte) (err error) {
	fmt.Println(string(payload))
	return
}

func main() {
	queue.Listen("localhost:11300", "test_queue", handler)
}
