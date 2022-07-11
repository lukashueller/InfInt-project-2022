import json

class CompanyJoiner():
    wd_companies = open('data/2_elastic_dumps/wd-companies.txt', 'r')
    rb_announcements = open('data/3_joined_data/enriched_rb_dump_with_company_name.txt', 'r')
    joined_file = open('data/3_joined_data/joined_companies.txt', 'w')

    allCompanies = dict()
    print(" --- START IMPORT WIKIDATA COMPANIES ---")
    for line in wd_companies: 
        company_item = json.loads(line[:-1])["_source"]
        company_name = company_item["label"]
        allCompanies[company_name] = company_item
    print(" ---- IMPORTED " + str(len(allCompanies)) + " COMPANIES")
    print(" --- END IMPORT WIKIDATA COMPANIES ---")

    def join_rb_announcements(self):
        print(" --- START JOINING RB ANNOUNCEMENTS ---")
        count = 0
        n_matches = 0
        for line in self.rb_announcements:
            #if count == 1000000 : break
            if count % 100000 == 0 : print(count)
            
            line = json.loads(line[:-1])
            cn = line["_source"]["company_name"]

            wd_company = self.allCompanies.get(cn, "")
            if wd_company != "":
                joinedCompany = line

                joinedCompany["_source"]["rb_state_code"] = ("DE-" + joinedCompany["_id"][0:2].upper())

                joinedCompany["_source"]["wd_id"] = wd_company["id"]
                joinedCompany["_source"]["wd_description"] = wd_company["description"]
                joinedCompany["_source"]["wd_aliases"] = wd_company["aliases"]
                joinedCompany["_source"]["wd_inception"] = wd_company["inception"]
                joinedCompany["_source"]["wd_country"] = wd_company["country"]
                joinedCompany["_source"]["wd_website"] = wd_company["website"]

                joinedCompany["_source"]["wd_ceos"] = wd_company["ceos"]
                joinedCompany["_source"]["wd_founders"] = wd_company["founders"]
                joinedCompany["_source"]["wd_chairpersons"] = wd_company["chairpersons"]

                joinedCompany["_source"]["wd_stockExchanges"] = wd_company["stockExchanges"]
                joinedCompany["_source"]["wd_employeeNumber"] = wd_company["employeeNumber"]

                joinedCompany["_source"]["wd_isin"] = wd_company["isin"]
                joinedCompany["_source"]["wd_isni"] = wd_company["isni"]
                joinedCompany["_source"]["wd_euTransparancyRegisterID"] = wd_company["euTransparancyRegisterID"]
                joinedCompany["_source"]["wd_germanLobbyregisterID"] = wd_company["germanLobbyregisterID"]
                joinedCompany["_source"]["wd_openCorporatesID"] = wd_company["openCorporatesID"]
                
                self.joined_file.writelines(json.dumps(joinedCompany) + "\n")

                n_matches += 1

            count += 1
        print(" --- START JOINING RB ANNOUNCEMENTS ---")
        print(" --- NUMBER OF MATCHES: " + str(n_matches))
