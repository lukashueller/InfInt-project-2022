import logging
import json

import dictionaries.py as wd_dicts

from build.gen.wd_company_pb2 import Wd_Company, Datapoint, Exchange, Employee
from wd_producer import WdProducer

log = logging.getLogger(__name__)

class WdExtractor:
    def __init__(self):
        self.producer = WdProducer()

    # Opens the file and reads all entries line by line. 
    # Then iterates over the array of all companies and 
    # extracts all properties for one persons from this entry.
    def read_dump(self):
        print(" --- START READ DUMP --- ")

        wikidata_dump = open('data/wd_companies_dump.txt', 'r')
        Lines = wikidata_dump.readlines()
        wikidata_dump.close()

        count = 0
        for line in Lines:
            # if count == 5 : break # JUST FOR DEBUGGING (Interrupt after 5 companies)

            count += 1
            self.extract(json.loads(line[:-2])) # The wikidata dump is one large JSON file, so every line ends with a ",". The [:-2] removes this last comma.
        
        print(" --- END READ DUMP --- ")

    # This method extracts all necessary fields from each entry of 
    # the JSON file and converts them to the "corporate" schema.
    def extract(self, line):
        corporate = Wd_Company()

        company_id = line.get("id")
        company_label = line["labels"].get("de", {}).get("value", None)
        company_desc = line["descriptions"].get("de", {}).get("value", None)
        company_aliases = list(map(lambda x: x.get("value"), line["aliases"].get("de", [])))
        company_inception = line["claims"].get("P571", [{}])[0].get("mainsnak", {}).get("datavalue", {}).get("value",{}).get("time", None)
        company_instanceof = list(
            map(lambda x: x.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id", None),
                line["claims"].get("P31", [])))
        company_country_id = line["claims"].get("P17", [{}])[0].get("mainsnak", {}).get("datavalue", {}).get("value",
                                                                                                                {}).get(
            "id", None)
        company_country = wd_dicts.countrydict.get(company_country_id, "https://www.wikidata.org/wiki/"+company_country_id) if company_country_id != None else None
        company_website = line["claims"].get("P856", [{}])[0].get("mainsnak", {}).get("datavalue", {}).get("value",None)

        company_ceos = list(
            map(lambda x:
                {
                    "id": x.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id", None),
                    "from": x.get("qualifiers",{}).get("P580",[{}])[0].get("datavalue",{}).get("value",{}).get("time",None),
                    "to": x.get("qualifiers",{}).get("P582",[{}])[0].get("datavalue",{}).get("value",{}).get("time",None)
                },
                line["claims"].get("P169", [])))

        company_founder = list(
            map(lambda x: 
                {
                    "id": x.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id", None),
                    "from": x.get("qualifiers",{}).get("P580",[{}])[0].get("datavalue",{}).get("value",{}).get("time",None),
                    "to": x.get("qualifiers",{}).get("P582",[{}])[0].get("datavalue",{}).get("value",{}).get("time",None)
                },
                line["claims"].get("P112", [])))

        company_chairperson = list(
            map(lambda x:
                {
                    "id": x.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id", None),
                    "from": x.get("qualifiers",{}).get("P580",[{}])[0].get("datavalue",{}).get("value",{}).get("time",None),
                    "to": x.get("qualifiers",{}).get("P582",[{}])[0].get("datavalue",{}).get("value",{}).get("time",None)
                },
                line["claims"].get("P488", [])))

        company_stockexchange = list(
            map(lambda x:
                {
                    "exchange": wd_dicts.exchangedict.get(x.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id", ""), "https://www.wikidata.org/wiki/" + x.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id", "")),
                    "ticker": x.get("qualifiers",{}).get("P249",[{}])[0].get("datavalue", {}).get("value", None),
                    "from": x.get("qualifiers",{}).get("P580",[{}])[0].get("datavalue",{}).get("value",{}).get("time",None),
                    "to": x.get("qualifiers",{}).get("P582",[{}])[0].get("datavalue",{}).get("value",{}).get("time",None)
                },
                line["claims"].get("P414", [])))

        company_employees = list(
            map(lambda x:
                {
                    "employees": x.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("amount", None),
                    "date": x.get("qualifiers",{}).get("P585",[{}])[0].get("datavalue", {}).get("value", {}).get("time",None)
                },
                line["claims"].get("P1128", [])))

        company_ISIN = line["claims"].get("P1278", [{}])[0].get("mainsnak", {}).get("datavalue", {}).get("value",None)
        company_ISNI = line["claims"].get("P213", [{}])[0].get("mainsnak", {}).get("datavalue", {}).get("value",None)
        company_EUTRID = line["claims"].get("P2657", [{}])[0].get("mainsnak", {}).get("datavalue", {}).get("value",None)
        company_GLRID = line["claims"].get("P10301", [{}])[0].get("mainsnak", {}).get("datavalue", {}).get("value",None)
        company_OCID = line["claims"].get("P1320", [{}])[0].get("mainsnak", {}).get("datavalue", {}).get("value",None)

        companyTypes = [
            "Q783794",  #company
            "Q891723",  #public company
            "Q4830453", #business
            "Q6881511", #enterprise
        ]
        if all( not companyTypes.__contains__(x) for x in company_instanceof ):
            return

        #print(line) # JUST FOR DEBUGGING

        if company_id is not None : corporate.id = company_id                               #1
        if company_label is not None : corporate.label = company_label                      #2
        if company_desc is not None : corporate.description = company_desc                  #3
        if company_aliases is not None : corporate.aliases.extend(company_aliases)          #4
        if company_inception is not None : corporate.inception = company_inception          #5
        if company_country is not None : corporate.country = company_country                #6
        if company_website is not None : corporate.website = company_website                #7

        if len(company_ceos) > 0 :                      #8
            for ceo in company_ceos :
                myEmployee = Employee()
                if ceo["id"] is not None : myEmployee.person_id = ceo["id"]
                if ceo["from"] is not None : myEmployee.time_from =  ceo["from"]
                if ceo["to"] is not None : myEmployee.time_to =  ceo["to"]
                corporate.ceos.append(myEmployee)
        
        if len(company_founder) > 0 :                  #9
            for founder in company_founder :
                myEmployee = Employee()
                if founder["id"] is not None : myEmployee.person_id = founder["id"]
                if founder["from"] is not None : myEmployee.time_from =  founder["from"]
                if founder["to"] is not None : myEmployee.time_to =  founder["to"]
                corporate.founders.append(myEmployee)
        
        if len(company_chairperson) > 0 :              #10
            for chairperson in company_chairperson :
                myEmployee = Employee()
                if chairperson["id"] is not None : myEmployee.person_id = chairperson["id"]
                if chairperson["from"] is not None : myEmployee.time_from =  chairperson["from"]
                if chairperson["to"] is not None : myEmployee.time_to =  chairperson["to"]
                corporate.chairpersons.append(myEmployee)

        if len(company_stockexchange) > 0 :           #11
            for exchange in company_stockexchange :
                myExchange = Exchange()
                if exchange["exchange"] is not None : myExchange.name =  exchange["exchange"]
                if exchange["ticker"] is not None : myExchange.symbol =  exchange["ticker"]
                if exchange["from"] is not None : myExchange.time_from =  exchange["from"]
                if exchange["to"] is not None : myExchange.time_to =  exchange["to"]
                corporate.stockExchanges.append(myExchange)

        if len(company_employees) > 0 :                 #12
            for measurement in company_employees:
                myDatapoint = Datapoint()
                if measurement["employees"] is not None : myDatapoint.number = measurement["employees"]
                if measurement["date"] is not None : myDatapoint.date = measurement["date"]
                corporate.employeeNumber.append(myDatapoint)

        if company_ISIN is not None : corporate.isin = company_ISIN                            #13
        if company_ISNI is not None : corporate.isni = company_ISNI                            #14
        if company_EUTRID is not None : corporate.euTransparancyRegisterID = company_EUTRID    #15
        if company_GLRID is not None :corporate.germanLobbyregisterID = company_GLRID          #16
        if company_OCID is not None : corporate.openCorporatesID = company_OCID                #17
        
        self.producer.produce(corporate=corporate) # push corporate to kafka
