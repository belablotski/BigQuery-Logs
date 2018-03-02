package bqldr

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/beloblotskiy/BigQuery-Logs/scorer"
	"google.golang.org/api/iterator"
)

const (
	bigQueryProjectID     = "bigqueryavb"
	bigQueryDataset       = "SBA"
	bigQueryTableLogFiles = "logfiles"
	bigQueryTableLogLines = "loglines"
)

var (
	bigQuerySchemas = map[string]bigquery.Schema{
		bigQueryTableLogFiles: bigquery.Schema{
			{Name: "Id", Required: true, Type: bigquery.IntegerFieldType},
			{Name: "Sys", Required: true, Type: bigquery.StringFieldType},
			{Name: "Path", Required: true, Type: bigquery.StringFieldType},
			{Name: "LastModifiedDt", Required: true, Type: bigquery.DateTimeFieldType},
			{Name: "Size", Required: true, Type: bigquery.IntegerFieldType},
		},

		bigQueryTableLogLines: bigquery.Schema{
			{Name: "FileId", Required: true, Type: bigquery.IntegerFieldType},
			{Name: "LineNum", Required: true, Type: bigquery.IntegerFieldType},
			{Name: "LineText", Required: true, Type: bigquery.StringFieldType},
		},
	}

	logfilesPK int64
)

// Returns map of tables used in this project (or nil if it's abesent)
func getOurTables(ctx context.Context, client *bigquery.Client) (*bigquery.Dataset, map[string]*bigquery.Table) {
	bqTables := make(map[string]*bigquery.Table)
	bqTables[bigQueryTableLogFiles] = nil
	bqTables[bigQueryTableLogLines] = nil

	dataset := client.Dataset(bigQueryDataset)
	tables := dataset.Tables(ctx)
	for {
		table, err := tables.Next()
		if err != nil {
			if strings.Contains(err.Error(), "no more items in iterator") {
				break
			} else {
				log.Panic(err)
			}
		}
		if _, ok := bqTables[table.TableID]; ok {
			bqTables[table.TableID] = table
		}
	}

	return dataset, bqTables
}

// Designed for single-thread execution
func createOurTablesIfNeeded(ctx context.Context) {
	client, err := bigquery.NewClient(ctx, bigQueryProjectID)
	defer client.Close()
	if err != nil {
		log.Panic(err)
	}

	dataset, bqTables := getOurTables(ctx, client)

	for tableID, tablePtr := range bqTables {
		if tablePtr == nil {
			t := dataset.Table(tableID)
			schema, ok := bigQuerySchemas[tableID]
			if !ok {
				log.Panicf("can't find schema for table %s", tableID)
			}
			if err := t.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
				log.Panic(err)
			}
			bqTables[tableID] = t
			log.Printf("Table %s.%s.%s has been created", bigQueryProjectID, bigQueryDataset, tableID)
		} else {
			log.Printf("Table %s.%s.%s already exists", bigQueryProjectID, bigQueryDataset, tableID)
		}
	}
}

// initLoader returns Client and Uploaders for BigQuery tables
func initLoader(ctx context.Context) (client *bigquery.Client, logfilesUploader *bigquery.Uploader, loglinesUploader *bigquery.Uploader) {
	client, err := bigquery.NewClient(ctx, bigQueryProjectID)
	if err != nil {
		log.Panic(err)
	}

	_, bqTables := getOurTables(ctx, client)
	if bqTables[bigQueryTableLogFiles] == nil || bqTables[bigQueryTableLogLines] == nil {
		log.Panicf("can't get uploader - table pointer is nil in the map %v", bqTables)
	}

	logfilesUploader = bqTables[bigQueryTableLogFiles].Uploader()
	loglinesUploader = bqTables[bigQueryTableLogLines].Uploader()
	return client, logfilesUploader, loglinesUploader
}

func upload(ctx context.Context, logfilesUploader *bigquery.Uploader, loglinesUploader *bigquery.Uploader, score scorer.ScoringResult, pk int64, systemName string) {
	id, err := strconv.ParseInt(time.Now().Format("20060102150405"), 10, 64)
	if err != nil {
		log.Panic(err)
	}

	fileid := id*10000 + pk
	filerec := &bigquery.ValuesSaver{Schema: bigQuerySchemas[bigQueryTableLogFiles], InsertID: "",
		Row: []bigquery.Value{fileid, systemName, score.FilePath, civil.DateTimeOf(score.ModTime), score.Size}}
	if err := logfilesUploader.Put(ctx, filerec); err != nil {
		log.Panic(err)
	}

	const chunkSize = 500
	lineValues := make([]*bigquery.ValuesSaver, 0, chunkSize)
	for n, linetext := range strings.Split(score.Content, "\n") {
		lineValues = append(lineValues, &bigquery.ValuesSaver{Schema: bigQuerySchemas[bigQueryTableLogLines], InsertID: "", Row: []bigquery.Value{fileid, n, linetext}})
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
func Upload(nWorkers int, systemName string, scores <-chan scorer.ScoringResult) <-chan bool {
	isReady := make(chan bool)
	ctx := context.Background()

	loader := func(ctx context.Context, n int, wg *sync.WaitGroup) {
		defer wg.Done()
		log.Printf("BigQuery loader #%d starts (project %s, dataset %s)", n, bigQueryProjectID, bigQueryDataset)
		cnt := 0
		client, logfilesUploader, loglinesUploader := initLoader(ctx)
		defer client.Close()
		for score := range scores {
			upload(ctx, logfilesUploader, loglinesUploader, score, atomic.AddInt64(&logfilesPK, 1), systemName)
			cnt++
		}
		log.Printf("BigQuery loader #%d ends: processed %d scoring records", n, cnt)
	}

	go func() {
		createOurTablesIfNeeded(ctx)
		var wg sync.WaitGroup
		wg.Add(nWorkers)
		for i := 1; i <= nWorkers; i++ {
			go loader(ctx, i, &wg)
		}
		wg.Wait()
		isReady <- true
		close(isReady)
	}()

	return isReady
}

// GetMaxLastModTime retrieves max(LastModifiedDt) from SBA.logfiles (returns nil if table is empty)
func GetMaxLastModTime(systemName string) *time.Time {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, bigQueryProjectID)
	if err != nil {
		log.Panic(err)
	}
	defer client.Close()

	q := client.Query(fmt.Sprintf("select * from (select max(LastModifiedDt) m from SBA.logfiles where sys='%s') t where t.m is not null", systemName))
	it, err := q.Read(ctx)
	if err != nil {
		log.Panic(err)
	}

	var values []bigquery.Value
	err = it.Next(&values)
	if err == iterator.Done {
		log.Printf("GetMaxLastModTime for system='%s' returns nil - no log files for the system in BigQuery", systemName)
		return nil
	}
	if err != nil {
		log.Panic(err)
	}
	//log.Printf("GetMaxLastModTime retrieved %v", values)
	t, err := time.ParseInLocation("2006-01-02 15:04:05.999999", bigquery.CivilDateTimeString(values[0].(civil.DateTime)), time.Local)
	if err != nil {
		log.Panic(err)
	}
	log.Printf("GetMaxLastModTime for system='%s', retrieved %v (parsed as local time)", systemName, t)
	return &t
}

// Bloking DML execution, panics on errors
func execDML(ctx context.Context, client *bigquery.Client, dml string) (rowsAffected int64) {
	log.Printf("Executing DML (may fail if stream buffer isn't empty - wait 90 minutes): %s", dml)
	q := client.Query(dml)
	job, err := q.Run(ctx)
	if err != nil {
		log.Panic(err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		log.Panic(err)
	}
	if status.Err() != nil {
		log.Panic(err)
	}
	rowsAffected = status.Statistics.Details.(*bigquery.QueryStatistics).NumDMLAffectedRows
	log.Printf("Query finished in %v, %d rows affected", status.Statistics.EndTime.Sub(status.Statistics.StartTime), rowsAffected)
	return rowsAffected
}

// PrepareCDC removes BigQuery log records starting from startCDCCapture for specified system
func PrepareCDC(systemName string, startCDCCapture time.Time) {
	log.Printf("Preparing CDC - remove all records for sys = '%s' starting from %v", systemName, startCDCCapture)
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, bigQueryProjectID)
	if err != nil {
		log.Panic(err)
	}
	defer client.Close()

	sql1 := fmt.Sprintf("delete from `SBA.loglines` l where l.fileid in (select id from SBA.logfiles f where f.sys = '%s' and LastModifiedDt>='%s')",
		systemName, startCDCCapture.Format("2006-01-02 15:04:05.999999"))
	execDML(ctx, client, sql1)

	sql2 := fmt.Sprintf("delete from SBA.logfiles f where f.sys = '%s' and LastModifiedDt>='%s'", systemName, startCDCCapture.Format("2006-01-02 15:04:05.999999"))
	execDML(ctx, client, sql2)
}
