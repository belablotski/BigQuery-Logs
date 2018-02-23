package main

import (
	"log"
	"time"

	"github.com/beloblotskiy/BigQuery-Logs/bqldr"
	"github.com/beloblotskiy/BigQuery-Logs/dmaker"
	"github.com/beloblotskiy/BigQuery-Logs/scanner"
	"github.com/beloblotskiy/BigQuery-Logs/scorer"
)

func main() {
	t0 := time.Now()
	p := ".\\test_data"
	//etlutils.PrintSR(dmaker.Decide(1, scorer.Score(10, scanner.Scan(p))))
	<-bqldr.Upload(7, dmaker.Decide(1, scorer.Score(10, scanner.Scan(p))))
	log.Printf("Execution time: %v", time.Since(t0))
}
