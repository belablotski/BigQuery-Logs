package bqldr

import (
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"google.golang.org/api/iterator"
)

func TestConnectivityAndRead(t *testing.T) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "bigqueryavb")
	if err != nil {
		t.Error(err)
	}
	defer client.Close()

	q := client.Query(strings.Replace(`
		#standardSQL
		SELECT *
		FROM 'bigquery-public-data.usa_names.usa_1910_2013'
		LIMIT 5
	`, "'", "`", -1))
	it, err := q.Read(ctx)
	if err != nil {
		t.Error(err)
	}

	cnt := 0
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Error(err)
		}
		log.Println(values)
		cnt++
	}
	t.Logf("Fetched %d records", cnt)
	if cnt != 5 {
		t.Errorf("Fetched %d records, but expectd 5", cnt)
	}
}

func TestTableCreationAndWrite(t *testing.T) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "bigqueryavb")
	if err != nil {
		t.Error(err)
	}
	defer client.Close()

	dataset := client.Dataset("SBA")
	if err := dataset.Create(ctx, nil); err != nil {
		if strings.Contains(err.Error(), "Already Exists: Dataset bigqueryavb:SBA, duplicate") {
			t.Log("Dataset SBA already exists")
		} else {
			t.Error(err)
		}
	}

	const tableName = "unittests_table1"
	table := dataset.Table(tableName)
	if err := table.Delete(ctx); err != nil {
		if strings.Contains(err.Error(), "Not found: Table ") {
			t.Logf("Can't delete %s - not found", tableName)
		} else {
			t.Error(err)
		}
	}

	// In this case it was better to create schema from struct since I use struct further...
	schema := bigquery.Schema{
		{Name: "Id", Required: true, Type: bigquery.IntegerFieldType},
		{Name: "Name", Required: true, Type: bigquery.StringFieldType},
		{Name: "Dt", Required: true, Type: bigquery.DateTimeFieldType},
	}

	if err := table.Create(ctx, &bigquery.TableMetadata{Schema: schema, ExpirationTime: time.Now().Add(5 * time.Minute)}); err != nil {
		t.Error(err)
	}

	uploader := table.Uploader()
	items := []*bigquery.ValuesSaver{
		&bigquery.ValuesSaver{Schema: schema, InsertID: "", Row: []bigquery.Value{1, "John",
			civil.DateTime{
				Date: civil.Date{Year: 2018, Month: 2, Day: 21},
				Time: civil.Time{Hour: 4, Minute: 5, Second: 6, Nanosecond: 7000}}}},
		&bigquery.ValuesSaver{Schema: schema, InsertID: "", Row: []bigquery.Value{2, "Jim",
			civil.DateTime{
				Date: civil.Date{Year: 2018, Month: 2, Day: 21},
				Time: civil.Time{Hour: 4, Minute: 5, Second: 6, Nanosecond: 7000}}}},
		&bigquery.ValuesSaver{Schema: schema, InsertID: "", Row: []bigquery.Value{2, "James",
			civil.DateTimeOf(time.Now())}},
	}

	if err := uploader.Put(ctx, items); err != nil {
		t.Error(err)
	}
}
