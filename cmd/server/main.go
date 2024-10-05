package main

import (
	"glog/internal/server"
	"log"
)

func main(){
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}