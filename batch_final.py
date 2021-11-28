import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv
import re

PROJECT_ID = 'quiet-sum-333023'
SCHEMA_RAW = 'id:STRING, name:STRING, host_id:STRING,	host_name:STRING, neighbourhood_group:STRING, ' \
         'neighbourhood:STRING, latitude:FLOAT, longitude:FLOAT, room_type:STRING, price:FLOAT, ' \
         'minimum_nights:INTEGER, number_of_reviews:INTEGER, last_review:STRING, reviews_per_month:FLOAT, ' \
         'calculated_host_listings_count:INTEGER, availability_365:INTEGER'

SCHEMA_GBN = 'neighbourhood:STRING, list_per_neighbourhood:INTEGER'

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    branched = beam.Pipeline(options=PipelineOptions())

    input_collection = (
        branched | 'ReadData' >> beam.io.ReadFromText('gs://springml_take_home/AB_NYC_2019_NLR.csv', 
                                                        skip_header_lines=1))
    
    raw_bq_pipe = (
        input_collection 
        | 'SplitData_Raw' >> beam.Map(lambda x: re.split('[,](?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', x))
        | 'FormatToDict_Raw' >> beam.Map(lambda x: {"id": x[0], "name": x[1], "host_id": x[2], "host_name": x[3],
                                               "neighbourhood_group": x[4], "neighbourhood": x[5], "latitude": x[6],
                                               "longitude": x[7], "room_type": x[8], "price": x[9],
                                               "minimum_nights": x[10], "number_of_reviews": x[11],
                                               "last_review": x[12], "reviews_per_month": x[13],
                                               "calculated_host_listings_count": x[14], "availability_365": x[15]})
        | 'WriteToBigQuery_Raw' >> beam.io.WriteToBigQuery(
           '{0}:AB_NYC_2019.AB_NYC_2019_RAW'.format(PROJECT_ID),           
           schema=SCHEMA_RAW,
           create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))

    group_by__transform_pipe = (
        input_collection
        | 'SplitData_GBN' >> beam.Map(lambda x: re.split('[,](?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', x)[5])
        | 'Map record to 1' >> beam.Map(lambda record: (record, 1))
        | 'GroupBy' >> beam.GroupByKey()
        | 'Sum using beam.Map' >> beam.Map(lambda record: (record[0], sum(record[1])))
        | 'FormatToDict_GBN' >> beam.Map(lambda x: {"neighbourhood": x[0], "list_per_neighbourhood": x[1]})
        | 'WriteToBigQuery_GBN' >> beam.io.WriteToBigQuery(
           '{0}:AB_NYC_2019.AB_NYC_2019_GroupBy_Neighbourhood'.format(PROJECT_ID),
           schema=SCHEMA_GBN,
           create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    
    output = ((raw_bq_pipe, group_by__transform_pipe))

    result = branched.run()
    result.wait_until_finish()
