# BigQuery Storage API example

In 2021, Google released a [new
API](http://cloud.google.com/go/bigquery/storage/apiv1beta2) for streaming data
into BigQuery tables. It is referred to variously as the "Storage API" and the
"Write API". This repository provides an example of how to use this API from
Golang to stream rows into a bigquery table.

First install the Google Cloud command line tools and authenticate with

```bash
gcloud auth application-default login
```

Next create a bigquery dataset and a table. In this example our table will contain two columns: a name and an age:

```bash
# create the dataset
bq mk mydataset

# create the table
bq --project_id=myproject mk -t mydataset.mytable name:string,age:integer
```

Next we stream data into the table. You may need to edit the "project" constant
at the top of the main source file in this repository.
```bash
go run *.go
```

The data that was streaming into the table is a constant at the top of the main
source file. You can now view it with:
```bash
$ bq query 'select * from mydataset.mytable'
Waiting on bqjob_r1b39442e5474a885_0000017df21f629e_1 ... (0s) Current status: DONE   
+------------+-----+
|    name    | age |
+------------+-----+
| John Doe   | 104 |
| Jane Doe   |  69 |
| Adam Smith |  33 |
+------------+-----+
```