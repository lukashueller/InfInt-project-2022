import logging
import json

from build.gen.wd_person_pb2 import Wd_Person
from wd_producer import WdProducer

log = logging.getLogger(__name__)

class WdExtractor:
    def __init__(self):
        self.producer = WdProducer()

    # Opens the file and reads all entries line by line. 
    # Then iterates over the array of all persons and 
    # extracts all properties for one person from this entry.
    def read_dump(self):
        print(" --- START READ DUMP --- ")

        wikidata_dump = open('data/wd_persons_dump.txt', 'r')
        Lines = wikidata_dump.readlines()
        wikidata_dump.close()

        count = 0
        for line in Lines:
            #if count == 100 : break # JUST FOR DEBUGGING (Interrupt after 5 persons)

            count += 1
            self.extract(json.loads(line))
        
        print(" --- END READ DUMP --- ")

    # This method extracts all necessary fields from each entry of 
    # the JSON file and converts them to the "wd-persons" schema.
    def extract(self, line):
        if "Q5" not in line["instanceof"] : 
            print("NOT A HUMAN")
            return 

        person = Wd_Person()

        #print(line) # JUST FOR DEBUGGING

        if line["id"] is not None : person.id = line["id"]                          #1
        if line["name"] is not None : person.name = line["name"]                    #2
        if line["birthdate"] is not None : person.date_birth = line["birthdate"]    #3
        if line["deathdate"] is not None : person.date_death = line["deathdate"]    #4

        self.producer.produce(person=person) # push corporate to kafka
