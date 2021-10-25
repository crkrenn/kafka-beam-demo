#!/usr/bin/env python3

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.external.kafka import ReadFromKafka, WriteToKafka
from apache_beam.io.textio import ReadAllFromText, WriteToText
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.coders.coders import BytesCoder
from apache_beam.transforms.util import WithKeys


# import argparse
# import logging
# import re
# import os

# from past.builtins import unicode

# import apache_beam as beam
# from apache_beam.io import ReadFromText, ReadAllFromText
# from apache_beam.io import WriteToText
# from apache_beam.options.pipeline_options import PipelineOptions
# from apache_beam.options.pipeline_options import SetupOptions



# classapache_beam.io.textio.ReadAllFromText(min_bundle_size=0, desired_bundle_size=67108864, compression_type='auto', strip_trailing_newlines=True, coder=StrUtf8Coder, skip_header_lines=0, **kwargs)[source]

# def run_pipeline():
#   with beam.Pipeline(options=PipelineOptions()) as p:
#     (p
#      # | 'Read from Kafka' >> ReadFromKafka(consumer_config={'bootstrap.servers': bootstrap_servers,
#                                                            # 'auto.offset.reset': 'latest'},
#                                           # topics=['numtest'])
#      | 'Read from Kafka' >> ReadAllFromText()["numbers.txt"]

#      # | 'Par with 1' >> beam.Map(lambda word: (word, 1))
#      # | 'Window of 10 seconds' >> beam.WindowInto(window.FixedWindows(1))
#      # | 'Group by key' >> beam.GroupByKey()
#      # | 'Sum word counts' >> beam.Map(lambda kv: (kv[0], sum(kv[1])))
#      | 'Write to Kafka' >> WriteToKafka(producer_config={'bootstrap.servers': bootstrap_servers},
#                                         topic='numtest')
#     )

bootstrap_servers = ['localhost:9092']

# content = [
#   ('a', 1),
#   ('b', 2),
#   ('c', 3),
#   ('d', 4)
# ]
# org.apache.kafka.common.serialization.LongSerializer
def run_pipeline():
    p = beam.Pipeline(options=PipelineOptions())
    numbers = p | beam.Create(["numbers.txt"])
    # numbers = p | beam.Create(content)

    (numbers
      | 'Read' >> ReadAllFromText(skip_header_lines=1)
      | 'Add keys' >> WithKeys("num")
      # | 'Write to Kafka' >> WriteToKafka(
        # producer_con?fig={'bootstrap.servers': bootstrap_servers},
                          # topic='numtest')
      | 'Write to Kafka' >> WriteToKafka(
             producer_config={'bootstrap.servers': bootstrap_servers},
             topic='numtest', 
             key_serializer='org.apache.kafka.common.serialization.StringSerializer', 
             value_serializer='org.apache.kafka.common.serialization.StringSerializer',
             expansion_service='localhost:8097')            
      | 'write' >> WriteToText(
             'numbers_out.txt')
             # coder=BytesCoder)
      )
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    run_pipeline()
