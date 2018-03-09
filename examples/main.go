package main

import (
	"log"

	"github.com/go-br/queue"
)

func main() {
	log.Println("Starting...")
	conn, err := queue.ConnectLoop("test_queue", 500)
	if err != nil {
		log.Fatal(err)
	}
	queue.Loop(conn)
}
