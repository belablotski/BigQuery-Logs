package scorer

import (
	"testing"
)

// TestLogParsing tests log parsing
func TestLogParsing(t *testing.T) {
	p := "testdata\\log1.log"
	sr := scoreFile(p)
	if sr.FilePath != p {
		t.Errorf("Invalid file path, expected '%s' but got '%s'", p, sr.FilePath)
	} else if sr.NumOfSbaMsgs != 1 {
		t.Errorf("Expected num of errors is 1, but got %d", sr.NumOfSbaMsgs)
	}

	p = "testdata\\log2.log"
	sr = scoreFile(p)
	if sr.FilePath != p {
		t.Errorf("Invalid file path, expected '%s' but got '%s'", p, sr.FilePath)
	} else if sr.NumOfSbaMsgs != 2 {
		t.Errorf("Expected num of errors is 2, but got %d", sr.NumOfSbaMsgs)
	}
}
