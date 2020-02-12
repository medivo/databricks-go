package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"

	"github.com/medivo/databricks-go"
)

var account = flag.String("account", "", "Databricks account")

func main() {
	flag.Parse()
	client, err := databricks.NewClient(
		*account,
		databricks.ClientHTTPClient(databricks.NewBearerHTTPClient("db_token")),
	)
	if err != nil {
		log.Fatalln(err)
	}

	ctx := context.Background()
	attrs, err := client.Cluster().List(ctx)
	if err != nil {
		log.Fatalln(err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.Encode(attrs)
}
