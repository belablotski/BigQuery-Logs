// Package scorer calculate score for each file
package scorer

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// ScoringResult contains scoring results for one file
type ScoringResult struct {
	FilePath     string
	Content      string
	ModTime      time.Time
	Size         int64
	NumOfSbaMsgs int
}

var (
	sbacliError = regexp.MustCompile("(?m)^\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d\\:\\d\\d\\:\\d\\d,\\d\\d\\d\\tsbacli\\t")
)

func isScoringNeeded(file string) bool {
	switch strings.ToLower(filepath.Ext(file)) {
	case "", ".log", ".err", ".out", ".stdout", ".stderr", ".txt":
		return true
	}
	return false
}

func scoreFile(file string) ScoringResult {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		// TODO: instead of skipping the file, log that the file is exist but it's impossible to read it
		if strings.Contains(err.Error(), "Access is denied") {
			log.Printf("ERROR: Can't read a file - access denied. Error: %s", err.Error())
		} else {
			log.Panic(err)
		}
	}
	info, err := os.Stat(file)
	if err != nil {
		log.Panic(err)
	}
	content := string(bytes)
	match := sbacliError.FindAllString(content, -1)
	if match == nil {
		return ScoringResult{file, content, info.ModTime(), info.Size(), 0}
	}
	return ScoringResult{file, content, info.ModTime(), info.Size(), len(match)}
}

// Score does parallel scoring calculations for files from input channel
func Score(nWorkers int, files <-chan string) <-chan ScoringResult {
	scores := make(chan ScoringResult, 100)

	scorer := func(n int, wg *sync.WaitGroup) {
		defer wg.Done()
		log.Printf("Scorer #%d starts", n)
		scored := 0
		skipped := 0
		for file := range files {
			if isScoringNeeded(file) {
				scores <- scoreFile(file)
				scored++
			} else {
				skipped++
			}
			if (scored+skipped)%1000 == 0 {
				log.Printf("Scorer #%d went thru %d files: processed %d files, skipped %d files", n, scored+skipped, scored, skipped)
			}
		}
		log.Printf("Scorer #%d ends: processed %d files, skipped %d files...", n, scored, skipped)
	}

	go func() {
		var wg sync.WaitGroup
		wg.Add(nWorkers)
		for i := 1; i <= nWorkers; i++ {
			go scorer(i, &wg)
		}
		wg.Wait()
		close(scores)
	}()

	return scores
}
