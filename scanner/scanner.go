// Package scanner scans directory tree
package scanner

import (
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

func isScoringNeeded(startCapture time.Time, path string, fi os.FileInfo) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case "", ".log", ".err", ".out", ".stdout", ".stderr", ".txt":
		log.Printf("File %s, modtime=%v, startCapture=%v", path, fi.ModTime(), startCapture)
		if !fi.ModTime().Before(startCapture) {
			return true
		}
	}
	return false
}

func listFiles(startTime time.Time, startDir string, files chan<- string) (scored int, skipped int) {
	filesAndDirs, err := ioutil.ReadDir(startDir)
	if err != nil {
		if strings.Contains(err.Error(), "Access is denied") {
			log.Println(err)
		} else {
			log.Panicln(err)
		}
	}

	for _, f := range filesAndDirs {
		p := path.Join(startDir, f.Name())
		if f.IsDir() {
			sco, ski := listFiles(startTime, p, files)
			scored += sco
			skipped += ski
		} else {
			if isScoringNeeded(startTime, p, f) {
				files <- p
				scored++
			} else {
				skipped++
			}
		}
	}
	return scored, skipped
}

// Scan does file system scan, starting from specified folder and submits found files into output channel if file last modification time equal or after startTime
func Scan(startTime time.Time, startDir string) <-chan string {
	files := make(chan string, 100)

	go func() {
		log.Printf("File system scanner starts from %s, CDC start date %v", startDir, startTime)
		scored, skipped := listFiles(startTime, startDir, files)
		close(files)
		log.Printf("File system scanner ends: %d files sent to scoring, %d files skipped", scored, skipped)
	}()

	return files
}
