package main

import (
	"log"
	"time"

	"github.com/beloblotskiy/data-signal-detector/dmaker"
	"github.com/beloblotskiy/data-signal-detector/etlutils"
	"github.com/beloblotskiy/data-signal-detector/scanner"
	"github.com/beloblotskiy/data-signal-detector/scorer"
)

func main() {
	t0 := time.Now()
	p := ".\\test_data"
	etlutils.PrintSR(dmaker.Decide(1, scorer.Score(10, scanner.Scan(p))))
	log.Printf("Execution time: %v", time.Since(t0))
}
