import re
import json

class CompanyNameExtractor():
    allCompanies = open('data/3_joined_data/enriched_rb_dump_with_company_name.txt', 'w')

    # Opens the file and reads all entries line by line. 
    # Then iterates over the array of all companies and 
    # extracts all properties for one persons from this entry.
    def read_dump(self):
        print(" --- START READ DUMP --- ")

        elastic_dump = open('data/2_elastic_dumps/corporate-events-dump.txt', 'r')
        Lines = elastic_dump.readlines()
        elastic_dump.close()

        count = 0
        for line in Lines:
            if count % 100000 == 0 : print(str(count))
            #if count == 5: break # JUST FOR DEBUGGING (Interrupt after 5 companies)

            count += 1
            self.modify(json.loads(line[:-1])) # The wikidata dump is one large JSON file, so every line ends with a ",". The [:-1] removes this last comma.
        
        print(" --- END READ DUMP --- ")

    def modify(self, line):
        rb_information = line["_source"]["information"]

        # extract RB-Number
        if rb_information.startswith("HR") : 
            splitHRB = re.search(r"^([\s\S]*?):(.*)", rb_information)
            try : 
                hrb_nr = splitHRB.group(1).strip()
                rb_information = splitHRB.group(2).strip()
            except : 
                return
        
        # extract company name
        split_company_name_and_rest = rb_information.split(",",1)
        company_name = split_company_name_and_rest[0]

        # extract other fields
        #found = False
        #for company in self.companiesArray : 
        #    if company["name"] == company_name :
        #        found = company["companyJoinNr"]
        #        break
        #if type(found) == Boolean : 
        #    newSourceItem["companyJoinNr"] = count
        #    self.companiesArray.append({"name" : company_name, "companyJoinNr" : count})
        #else : 
        #    newSourceItem["companyJoinNr"] = found

        # write new rb_dump_file
        newSourceItem = line["_source"]
        newSourceItem["company_name"] = company_name
        newLineItem = {"_index" : line["_index"], "_type" : line["_type"], "_id" : line["_id"], "_score" : line["_score"], "_source" : newSourceItem}
        self.allCompanies.writelines(json.dumps(newLineItem) + "\n")

