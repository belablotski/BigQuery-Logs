package etlutils

import (
	"fmt"
	"time"

	"github.com/beloblotskiy/BigQuery-Logs/bqldr"
	"github.com/beloblotskiy/BigQuery-Logs/scorer"
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

// CalcCDSStartDate calculates Change Data Capture start time for specified system
func CalcCDSStartDate(systemName string) time.Time {
	maxLasModDt := bqldr.GetMaxLastModTime(systemName)
	if maxLasModDt == nil {
		return time.Date(2016, 1, 1, 0, 0, 0, 0, time.Local)
	}
	t := time.Date(maxLasModDt.Year(), maxLasModDt.Month(), maxLasModDt.Day(), 0, 0, 0, 0, time.Local)
	return t.AddDate(0, 0, -1)
}
