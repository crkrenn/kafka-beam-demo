#!/usr/bin/env python3

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.external.kafka import ReadFromKafka, WriteToKafka

bootstrap_servers = ['localhost:9092']

def run_pipeline():
  with beam.Pipeline(options=PipelineOptions()) as p:
    (p
     | 'Read from Kafka' >> ReadFromKafka(consumer_config={'bootstrap.servers': bootstrap_servers,
                                                           'auto.offset.reset': 'latest'},
                                          topics=['numtest'])
     # | 'Par with 1' >> beam.Map(lambda word: (word, 1))
     | 'Window of 10 seconds' >> beam.WindowInto(window.FixedWindows(1))
     # | 'Group by key' >> beam.GroupByKey()
     # | 'Sum word counts' >> beam.Map(lambda kv: (kv[0], sum(kv[1])))
     | 'Write to Kafka' >> WriteToKafka(producer_config={'bootstrap.servers': bootstrap_servers},
                                        topic='numtest_out')
    )

if __name__ == '__main__':
  run_pipeline()
