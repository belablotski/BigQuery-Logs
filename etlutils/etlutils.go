package etlutils

import (
	"fmt"

	"github.com/beloblotskiy/data-signal-detector/scorer"
)

// Print prints all elements from channel ch
func Print(ch <-chan interface{}) {
	n := 0
	for i := range ch {
		n++
		switch i := i.(type) {
		case string:
			fmt.Printf("%d\t%q\n", n, i)
		default:
			fmt.Printf("%d\t%T\t%v\n", n, i, i)
		}
	}
}

// PrintS prints all elements from string channel
func PrintS(ch <-chan string) {
	chi := make(chan interface{})
	defer close(chi)
	go Print(chi)
	for s := range ch {
		chi <- s
	}
}

// PrintSR prints all elements from channel
func PrintSR(ch <-chan scorer.ScoringResult) {
	chi := make(chan interface{})
	defer close(chi)
	go Print(chi)
	for s := range ch {
		chi <- s
	}
}
