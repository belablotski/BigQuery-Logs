package bqldr

import (
	"context"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/beloblotskiy/BigQuery-Logs/scorer"
)

const (
	bigQueryProjectID = "bigqueryavb"
	bigQueryDataset   = "SBA"
)

var (
	logfilesSchema = bigquery.Schema{
		{Name: "Id", Required: true, Type: bigquery.IntegerFieldType},
		{Name: "Serv", Required: true, Type: bigquery.StringFieldType},
		{Name: "Path", Required: true, Type: bigquery.StringFieldType},
		{Name: "LastModifiedDt", Required: true, Type: bigquery.DateTimeFieldType},
		{Name: "Size", Required: true, Type: bigquery.IntegerFieldType},
	}

	loglinesSchema = bigquery.Schema{
		{Name: "FileId", Required: true, Type: bigquery.IntegerFieldType},
		{Name: "LineNum", Required: true, Type: bigquery.IntegerFieldType},
		{Name: "LineText", Required: true, Type: bigquery.StringFieldType},
	}

	logfilesPK int64
)

func initLoader(ctx context.Context) (client *bigquery.Client, logfilesUploader *bigquery.Uploader, loglinesUploader *bigquery.Uploader) {
	client, err := bigquery.NewClient(ctx, bigQueryProjectID)
	if err != nil {
		log.Panic(err)
	}

	dataset := client.Dataset(bigQueryDataset)
	logfilesTableName := "logfiles"
	logfiles := dataset.Table(logfilesTableName)
	if err := logfiles.Create(ctx, &bigquery.TableMetadata{Schema: logfilesSchema}); err != nil {
		if strings.Contains(err.Error(), "Already Exists: Table") {
			log.Printf("Table %s.%s alredy exists", bigQueryDataset, logfilesTableName)
		} else {
			log.Panic(err)
		}
	}

	loglinesTableName := "loglines"
	loglines := dataset.Table(loglinesTableName)
	if err := loglines.Create(ctx, &bigquery.TableMetadata{Schema: loglinesSchema}); err != nil {
		if strings.Contains(err.Error(), "Already Exists: Table") {
			log.Printf("Table %s.%s alredy exists", bigQueryDataset, loglinesTableName)
		} else {
			log.Panic(err)
		}
	}

	logfilesUploader = logfiles.Uploader()
	loglinesUploader = loglines.Uploader()
	return client, logfilesUploader, loglinesUploader
}

func upload(ctx context.Context, logfilesUploader *bigquery.Uploader, loglinesUploader *bigquery.Uploader, score scorer.ScoringResult, pk int64) {
	id, err := strconv.ParseInt(time.Now().Format("20060102150405"), 10, 64)
	if err != nil {
		log.Panic(err)
	}

	fileid := id*10000 + pk
	filerec := &bigquery.ValuesSaver{Schema: logfilesSchema, InsertID: "",
		Row: []bigquery.Value{fileid, "AVBTEST", score.FilePath, civil.DateTimeOf(score.ModTime), score.Size}}
	if err := logfilesUploader.Put(ctx, filerec); err != nil {
		log.Panic(err)
	}

	const chunkSize = 500
	lineValues := make([]*bigquery.ValuesSaver, 0, chunkSize)
	for n, linetext := range strings.Split(score.Content, "\n") {
		lineValues = append(lineValues, &bigquery.ValuesSaver{Schema: loglinesSchema, InsertID: "", Row: []bigquery.Value{fileid, n, linetext}})
		if n%chunkSize == 0 {
			if err := loglinesUploader.Put(ctx, lineValues); err != nil {
				log.Panic(err)
			}
			lineValues = make([]*bigquery.ValuesSaver, 0, chunkSize)
		}
	}
	if len(lineValues) > 0 {
		if err := loglinesUploader.Put(ctx, lineValues); err != nil {
			log.Panic(err)
		}
	}
}

// Upload writes resutls into BigQuery table, at the end it drops true into output channel
func Upload(nWorkers int, scores <-chan scorer.ScoringResult) <-chan bool {
	isReady := make(chan bool)
	ctx := context.Background()

	loader := func(n int, wg *sync.WaitGroup) {
		defer wg.Done()
		log.Printf("BigQuery loader #%d starts (project %s, dataset %s)", n, bigQueryProjectID, bigQueryDataset)
		cnt := 0
		client, logfilesUploader, loglinesUploader := initLoader(ctx)
		defer client.Close()
		for score := range scores {
			upload(ctx, logfilesUploader, loglinesUploader, score, atomic.AddInt64(&logfilesPK, 1))
			cnt++
		}
		log.Printf("BigQuery loader #%d ends, processed %d scoring records", n, cnt)
	}

	go func() {
		var wg sync.WaitGroup
		wg.Add(nWorkers)
		for i := 1; i <= nWorkers; i++ {
			go loader(i, &wg)
		}
		wg.Wait()
		isReady <- true
		close(isReady)
	}()

	return isReady
}
