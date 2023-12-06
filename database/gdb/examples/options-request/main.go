package main

import (
	"github.com/Darker-D/ddbase/database/gdb/gdbclient/graph"
	"context"
	"flag"
	"log"

	goClient "github.com/Darker-D/ddbase/database/gdb/gdbclient"
)

var (
	host, username, password string
	port                     int
)

func main() {
	flag.StringVar(&host, "host", "", "GDB Connection Host")
	flag.StringVar(&username, "username", "", "GDB username")
	flag.StringVar(&password, "password", "", "GDB password")
	flag.IntVar(&port, "port", 8182, "GDB Connection Port")
	flag.Parse()

	if host == "" || username == "" || password == "" {
		log.Fatal("No enough args provided. Please run:" +
			" go run main.go -host <gdb host> -username <username> -password <password> -port <gdb port>")
		return
	}

	settings := &goClient.Settings{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
	}

	// connect GDB with auth
	client := goClient.NewClient(settings)

	// send script dsl with bindings to GDB
	bindings := make(map[string]interface{})
	bindings["GDB___label"] = "goTest"
	bindings["GDB___PK"] = "name"
	bindings["GDB___PV"] = "Jack"

	dsl := "g.addV(GDB___label).property(GDB___PK, GDB___PV)"
	options := graph.NewRequestOptionsWithBindings(bindings)

	// set dsl evaluationTimeout to 3000ms
	options.SetTimeout(3000)
	results, err := client.SubmitScriptOptions(context.Background(), dsl, options)
	if err != nil {
		log.Fatalf("Error while querying: %s\n", err.Error())
	}

	// get response, add vertex should return a Vertex
	for _, result := range results {
		v := result.GetVertex()
		log.Printf("get vertex: %s", v.String())

		// read vertex property
		for _, p := range v.VProperties() {
			log.Printf("prop: %s", p.String())
		}
	}

	client.Close(context.Background())
}
