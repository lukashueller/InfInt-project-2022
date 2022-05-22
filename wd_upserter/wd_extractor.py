import logging
import json
from time import sleep

from build.gen.bakdata.corporate.v1.wd_company_pb2 import Wd_Company, Status
from wd_producer import WdProducer

log = logging.getLogger(__name__)

class WdExtractor:
    def __init__(self):
        self.producer = WdProducer()

    def read_dump(self):
        print("READ DUMP")

        wikidata_dump = open('data/wikidata_dump.txt', 'r')
        Lines = wikidata_dump.readlines()
        wikidata_dump.close()

        count = 0
        for line in Lines:
            if count == 5:
                break

            count += 1
            self.extract(json.loads(line))


    def extract(self, line):
        corporate = Wd_Company()

        print(line)
        
        corporate.id = line["id"]
        corporate.label = line["label"]
        corporate.country = line["country"]
        corporate.website = line["website"]

        # print(corporate)
        
        self.producer.produce(corporate=corporate)

        # exit(0)
