# Google_Dataflow_Big_Query_Pipeline
Repo for Data Engineer Personal Project

## Project Description

Create a pipeline in Dataflow that reads data from a csv file, applies transformations, and inserts resulting data into BigQuery table. 

___Pipeline requirements are as follows:___

1.  Download and store NYC Airbnb dataset available here (https://www.kaggle.com/dgomonov/new-york-city-airbnb-open-data) in a GCS bucket.
2.  Create a Dataflow batch job in Python that can read and process this file.
3.  In the Dataflow job, apply a "Group By" transform to get the count of listings by the "neighbourhood" field.
4.  Store both the original csv data and the transformed data into their own separate BigQuery tables.
