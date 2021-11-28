import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv
import re

PROJECT_ID = 'quiet-sum-333023'
SCHEMA = 'neighbourhood:STRING, list_per_neighbourhood:INTEGER'


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromText('gs://springml_take_home/AB_NYC_2019_NLR.csv', skip_header_lines=1)
       | 'SplitData' >> beam.Map(lambda x: re.split('[,](?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', x)[5])
       | 'Map record to 1' >> beam.Map(lambda record: (record, 1))
       | 'GroupBy' >> beam.GroupByKey()
       | 'Sum using beam.Map' >> beam.Map(lambda record: (record[0], sum(record[1])))
       | 'FormatToDict' >> beam.Map(lambda x: {"neighbourhood": x[0], "list_per_neighbourhood": x[1]})
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:AB_NYC_2019_RAW.AB_NYC_2019_GroupBy_Neighbourhood'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()
    result.wait_until_finish()
