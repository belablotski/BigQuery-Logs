package bqldr

import (
	"context"
	"log"

	"cloud.google.com/go/bigquery"
)

func f() {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "BigQueryAvb")
	if err != nil {
		// TODO: Handle error.
	}
	log.Println(client)
}
