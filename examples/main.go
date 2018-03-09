package main

import (
	"log"

	"github.com/go-br/queue"
)

func main() {
	log.Println("Starting...")
	conn, err := queue.ConnectLoop("publicapi_webhook", 500)
	if err != nil {
		log.Fatal(err)
	}
	queue.Loop(conn)
}
