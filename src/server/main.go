package main

import (
	"fmt"
	"net/http"
	"time"
)

import "github.com/paulbellamy/ratecounter"

func handlerWithCounter(counter *ratecounter.RateCounter) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		counter.Incr(1)
		fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	//counter.Incr(1)
	fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
}

func printRate(counter *ratecounter.RateCounter) {
	for _ = range time.Tick(5 * time.Second) {
		fmt.Println(counter.Rate()/10, "req/s")
	}
}

func main() {
	counter := ratecounter.NewRateCounter(10 * time.Second)
	go printRate(counter)
	http.HandleFunc("/", handlerWithCounter(counter))
	http.ListenAndServe(":8080", nil)
}
