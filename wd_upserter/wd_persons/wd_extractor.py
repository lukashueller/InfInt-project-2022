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
            self.extract(json.loads(line[:-2])) # The wikidata dump is one large JSON file, so every line ends with a ",". The [:-2] removes this last comma.
        
        print(" --- END READ DUMP --- ")

    # This method extracts all necessary fields from each entry of 
    # the JSON file and converts them to the "wd-persons" schema.
    def extract(self, line):
        person_instanceof = list(map(lambda x: x.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id", None) ,line["claims"].get("P31", [])))
        
        if "Q5" not in person_instanceof : 
            print("NOT A HUMAN")
            return 

        person_id = line["id"]
        person_name = line["labels"].get("de", {}).get("value", None)
        person_birthdate = line["claims"].get("P569", [{}])[0].get("mainsnak",{}).get("datavalue",{}).get("value",{}).get("time", None)
        person_deathdate = line["claims"].get("P570", [{}])[0].get("mainsnak",{}).get("datavalue",{}).get("value",{}).get("time", None)

        person = Wd_Person()

        #print(line) # JUST FOR DEBUGGING

        if person_id is not None : person.id = person_id                          #1
        if person_name is not None : person.name = person_name                    #2
        if person_birthdate is not None : person.date_birth = person_birthdate    #3
        if person_deathdate is not None : person.date_death = person_deathdate    #4

        self.producer.produce(person=person) # push corporate to kafka
