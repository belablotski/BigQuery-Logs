package bqldr

import (
	"context"
	"log"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

func TestConnectivity(t *testing.T) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "bigqueryavb")
	if err != nil {
		t.Error(err)
	}

	q := client.Query(strings.Replace(`
		SELECT year, SUM(number) as num
		FROM 'bigquery-public-data.usa_names.usa_1910_2013'
		WHERE name = "William"
		GROUP BY year
		ORDER BY year
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
}
