import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv
import re

PROJECT_ID = 'quiet-sum-333023'
SCHEMA = 'id:STRING, name:STRING, host_id:STRING,	host_name:STRING, neighbourhood_group:STRING, ' \
         'neighbourhood:STRING, latitude:FLOAT, longitude:FLOAT, room_type:STRING, price:FLOAT, ' \
         'minimum_nights:INTEGER, number_of_reviews:INTEGER, last_review:STRING, reviews_per_month:FLOAT, ' \
         'calculated_host_listings_count:INTEGER, availability_365:INTEGER'


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromText('gs://springml_take_home/AB_NYC_2019_NLR.csv', skip_header_lines=1)
       | 'SplitData' >> beam.Map(lambda x: re.split('[,](?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', x))
       | 'FormatToDict' >> beam.Map(lambda x: {"id": x[0], "name": x[1], "host_id": x[2], "host_name": x[3],
                                               "neighbourhood_group": x[4], "neighbourhood": x[5], "latitude": x[6],
                                               "longitude": x[7], "room_type": x[8], "price": x[9],
                                               "minimum_nights": x[10], "number_of_reviews": x[11],
                                               "last_review": x[12], "reviews_per_month": x[13],
                                               "calculated_host_listings_count": x[14], "availability_365": x[15]})
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:AB_NYC_2019_RAW.AB_NYC_2019_RAW'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()
    result.wait_until_finish()
