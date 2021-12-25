//go:generate protoc -I/usr/local/include -I. --go_out=. row.proto

package main

import (
	"context"
	"fmt"
	"log"

	storage "cloud.google.com/go/bigquery/storage/apiv1beta2"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1beta2"
	"google.golang.org/protobuf/proto"
)

const (
	project = "myproject"
	dataset = "mydataset"
	table   = "mytable"
	trace   = "bigquery-writeclient-example" // identifies this client for bigquery debugging
)

// the data we will stream to bigquery
var rows = []*Row{
	{Name: "John Doe", Age: 104},
	{Name: "Jane Doe", Age: 69},
	{Name: "Adam Smith", Age: 33},
}

func main() {
	ctx := context.Background()

	// create the bigquery client
	client, err := storage.NewBigQueryWriteClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// create the write stream
	// a COMMITTED write stream inserts data immediately into bigquery
	resp, err := client.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent: fmt.Sprintf("projects/%s/datasets/%s/tables/%s", project, dataset, table),
		WriteStream: &storagepb.WriteStream{
			Type: storagepb.WriteStream_COMMITTED,
		},
	})
	if err != nil {
		log.Fatal("CreateWriteStream: ", err)
	}

	// get the stream by calling AppendRows
	stream, err := client.AppendRows(ctx)
	if err != nil {
		log.Fatal("AppendRows: ", err)
	}

	// get the protobuf descriptor for our row type
	var row Row
	descriptor, err := adapt.NormalizeDescriptor(row.ProtoReflect().Descriptor())
	if err != nil {
		log.Fatal("NormalizeDescriptor: ", err)
	}

	// serialize the rows
	var opts proto.MarshalOptions
	var data [][]byte
	for _, row := range rows {
		buf, err := opts.Marshal(row)
		if err != nil {
			log.Fatal("protobuf.Marshal: ", err)
		}
		data = append(data, buf)
	}

	// send the rows to bigquery
	err = stream.Send(&storagepb.AppendRowsRequest{
		WriteStream: resp.Name,
		TraceId:     trace, // identifies this client
		Rows: &storagepb.AppendRowsRequest_ProtoRows{
			ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
				// protocol buffer schema
				WriterSchema: &storagepb.ProtoSchema{
					ProtoDescriptor: descriptor,
				},
				// protocol buffer data
				Rows: &storagepb.ProtoRows{
					SerializedRows: data, // serialized protocol buffer data
				},
			},
		},
	})
	if err != nil {
		log.Fatal("AppendRows.Send: ", err)
	}

	// get the response, which will tell us whether it worked
	_, err = stream.Recv()
	if err != nil {
		log.Fatal("AppendRows.Recv: ", err)
	}

	log.Println("done")
}
