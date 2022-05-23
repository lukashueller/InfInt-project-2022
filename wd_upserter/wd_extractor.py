import logging
import json
from time import sleep

from build.gen.wd_company_pb2 import Wd_Company, Datapoint, Exchange, Employee
from wd_producer import WdProducer

log = logging.getLogger(__name__)

class WdExtractor:
    def __init__(self):
        self.producer = WdProducer()

    # Opens the file and reads all entries line by line. 
    # Then iterates over the array of all companies and 
    # extracts all properties for one company from this entry.
    def read_dump(self):
        print(" --- START READ DUMP --- ")

        wikidata_dump = open('data/wikidata_dump.txt', 'r')
        Lines = wikidata_dump.readlines()
        wikidata_dump.close()

        count = 0
        for line in Lines:
            if count == 5 : break # JUST FOR DEBUGGING (Interrupt after 5 companies)

            count += 1
            self.extract(json.loads(line))
        
        print(" --- END READ DUMP --- ")

    # This method extracts all necessary fields from each entry of 
    # the JSON file and converts them to the "corporate" schema.
    def extract(self, line):
        corporate = Wd_Company()

        #print(line) # JUST FOR DEBUGGING

        corporate.id = line["id"]                       #1
        corporate.label = line["label"]                 #2
        corporate.description = line["description"]     #3
        corporate.aliases.extend(line["aliases"])       #4
        corporate.inception = line["inception"]         #5
        corporate.country = line["country"]             #6
        corporate.website = line["website"]             #7

        if len(line["CEOs"]) > 0 :                      #8
            ceos = line["CEOs"]
            for ceo in ceos :
                myEmployee = Employee()
                if ceo["id"] is not None : myEmployee.person_id = ceo["id"]
                if ceo["from"] is not None : myEmployee.time_from =  ceo["from"]
                if ceo["to"] is not None : myEmployee.time_to =  ceo["to"]
                corporate.ceos.append(myEmployee)
        
        if len(line["Founders"]) > 0 :                  #9
            founders = line["Founders"]
            for founder in founders :
                myEmployee = Employee()
                if founder["id"] is not None : myEmployee.person_id = founder["id"]
                if founder["from"] is not None : myEmployee.time_from =  founder["from"]
                if founder["to"] is not None : myEmployee.time_to =  founder["to"]
                corporate.founders.append(myEmployee)
        
        if len(line["Chairpersons"]) > 0 :              #10
            chairpersons = line["Chairpersons"]
            for chairperson in chairpersons :
                myEmployee = Employee()
                if chairperson["id"] is not None : myEmployee.person_id = chairperson["id"]
                if chairperson["from"] is not None : myEmployee.time_from =  chairperson["from"]
                if chairperson["to"] is not None : myEmployee.time_to =  chairperson["to"]
                corporate.chairpersons.append(myEmployee)

        if len(line["Stock Exchanges"]) > 0 :           #11
            exchanges = line["Stock Exchanges"]
            for exchange in exchanges :
                myExchange = Exchange()
                if exchange["Exchange"] is not None : myExchange.name =  exchange["Exchange"]
                if exchange["Ticker"] is not None : myExchange.symbol =  exchange["Ticker"]
                if exchange["from"] is not None : myExchange.time_from =  exchange["from"]
                if exchange["to"] is not None : myExchange.time_to =  exchange["to"]
                corporate.stockExchanges.append(myExchange)

        if len(line["Employees"]) > 0 :                 #12
            employees = line["Employees"]
            for employee in employees:
                myDatapoint = Datapoint()
                if employee["Employees"] is not None : myDatapoint.number =  employee["Employees"]
                if employee["Date"] is not None : myDatapoint.date = employee["Date"]
                corporate.employeeNumber.append(myDatapoint)

        references = line["References"]
        if references["ISIN"] is not None : corporate.isin = references["ISIN"]             #13
        if references["ISNI"] is not None : corporate.isni = references["ISNI"]             #14
        if references["EU Transparency Register ID"] is not None :                          #15
            corporate.euTransparancyRegisterID = references["EU Transparency Register ID"]
        if references["German Lobbyregister ID"] is not None :                              #16
            corporate.germanLobbyregisterID = references["German Lobbyregister ID"]
        if references["OpenCorporates ID"] is not None :                                    #17
            corporate.openCorporatesID = references["OpenCorporates ID"]        
        
        self.producer.produce(corporate=corporate) # push corporate to kafka
