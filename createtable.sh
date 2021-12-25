#!/bin/bash

PROJECT=myproject
DATASET=mydataset
TABLE=mytable
SCHEMA=name:string,age:integer

# create the dataset
bq mk ${DATASET}

# create the table
bq --project_id=${PROJECT} mk -t ${DATASET}.${TABLE} ${SCHEMA}
