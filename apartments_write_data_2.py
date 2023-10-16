from apartments_scrape_1 import INPUT_FILE
from apartments_scrape_1 import DATABASE_NAME
from apartments_scrape_1 import BATCH_SIZE
from apartments_scrape_1 import ApartmentsScraper


# install: requests, lxml
import requests
from lxml import html
import os
import sys
import csv
import threading
import bz2
import sqlite3
import pprint
import time
from datetime import datetime
from urllib.parse import urljoin


class ApartmentsParser(ApartmentsScraper):
    def write_data(self):
        if self.inputs_are_good == False or self.is_interrupted == True:
            return
        
        print("Writing data...")
        if not os.path.exists(self.input_file):
            print("Input file doesn't exist!")
            return
        
        if not self.input_file.lower().endswith(".csv"):
            print("Input file isn't a csv file!")
            return

        # if still here, should be good to read the file
        outfile_name = self.input_file[0:self.input_file.rfind(".")] + "_output.csv"
        outfile = open(outfile_name, 'w', newline='', encoding='utf-8')
        writer = csv.writer(outfile, delimiter=",", quoting=csv.QUOTE_MINIMAL)

        # read inputs, fetch data and parse
        ADDITIONAL_HEADERS = ['Item Type', 'Year Built', 'Number of Units and Stories', 'Status', 'Address', 'Floor Plan', 'Bed', 'Bath', 'Unit', 'Price', 'SF', 'Available', 'Move-in Special', 'Time of Scraping']
        link_index = None
        expected_line_length = None
        header_to_look_for = 'link'
        total_parsed = 0
        
        with open(self.input_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=",")
            is_header_row = True
            
            for line in reader:
                if is_header_row == True:
                    if header_to_look_for in line:
                        link_index = line.index(header_to_look_for)
                        
                    is_header_row = False
                    expected_line_length = len(line)
                    ccc = writer.writerow(line + ADDITIONAL_HEADERS)
                    continue

                # any data line is here!
                if link_index == None:
                    print("Can't find header in csv:", header_to_look_for)
                    break

                if len(line) != expected_line_length:
                    print("Unexpected line length for a line in input file!")
                    total_parsed+=1
                    continue

                # fetch data
                link_to_fetch = None
                try:
                    link_to_fetch = line[link_index]
                except IndexError:
                    pass

                if link_to_fetch == None:
                    # just write out empty
                    ccc = writer.writerow(line + ['']*len(ADDITIONAL_HEADERS))
                else:
                    # try to fetch data
                    fetched_data = self.db_cursor.execute("SELECT * FROM DataTable WHERE link=?", (link_to_fetch,)).fetchone()
                    if fetched_data == None: # not scraped
                        ccc = writer.writerow(line + ['']*len(ADDITIONAL_HEADERS))
                    else:
                        # parse out and write!
                        parsed_data = self.parse_data(fetched_data[0], fetched_data[1], fetched_data[2])
                        #pprint.pprint(parsed_data)
                        if len(parsed_data) != 0:
                            for item_to_write in parsed_data:
                                # write, now there are different types so some headers might be empty so need to iterate all headers
                                row_to_write = [original_item for original_item in line]
                                for header_item in ADDITIONAL_HEADERS:
                                    if header_item in item_to_write:
                                        row_to_write.append(item_to_write[header_item])
                                    else:
                                        row_to_write.append("")

                                ccc = writer.writerow(row_to_write)
                        else: # just write empty
                            ccc = writer.writerow(line + ['']*len(ADDITIONAL_HEADERS))

                total_parsed+=1
                if total_parsed%10000 == 0:
                    print("Total parsed so far:", total_parsed)

        outfile.close()
        print("Created output file:", outfile_name)
        return


    def parse_data(self, input_url, input_html, input_scrapetime):
        data_to_return = []

        if input_html != None:
            tree = html.fromstring(bz2.decompress(input_html))

            # get main address
            main_address = ""
            mainadr_el = tree.xpath("//div[@class='profileContent']/header[@id='profileHeaderWrapper']/div[@id='propertyHeader']//div[@class='propertyAddressContainer']/h2")
            if len(mainadr_el) != 0:
                # remove out district
                district_el = mainadr_el[0].xpath("./span[@class='neighborhoodAddress']")
                if len(district_el) != 0:
                    mainadr_el[0].remove(district_el[0])
                main_address = self.fix_string(mainadr_el[0].text_content())
                #print([main_address])

            # get move-in special
            move_in_special = ""
            moveinspec_el = tree.xpath("//div[@class='profileContent']/section[@id='rentSpecialsSection']/div[contains(@class, 'moveInSpecialsContainer')]/p")
            if len(moveinspec_el) != 0:
                move_in_special = moveinspec_el[0].text_content().strip()


            # get some more general property info
            general_property_info = {"Year Built":"", "Number of Units and Stories":"", "Status":""}
            misc_propinfo_els = tree.xpath("//div[@class='profileContent']/div/section[@id='feesSection']//h4[contains(text(), 'Property Information')]/../following-sibling::div[@class='component-body']/ul/li")
            for misc_propinfo_el_index, misc_propinfo_el in enumerate(misc_propinfo_els):
                current_misc_propinfo_text = ""
                current_misc_propinfo_text_lower = ""
                current_misc_propinfo_data_el = misc_propinfo_el.xpath("./div[contains(@class, 'component-row')]/div[contains(@class, 'column')]")
                if len(current_misc_propinfo_data_el) != 0:
                    current_misc_propinfo_text = current_misc_propinfo_data_el[0].text_content().strip()
                    current_misc_propinfo_text_lower = current_misc_propinfo_text.lower()

                # check which one it is
                if "built in " in current_misc_propinfo_text_lower:
                    general_property_info["Year Built"] = current_misc_propinfo_text[current_misc_propinfo_text_lower.find("built in ")+len("built in ") : ].strip()
                elif "units" in current_misc_propinfo_text_lower and ("stories" in current_misc_propinfo_text_lower or "story" in current_misc_propinfo_text_lower):
                    general_property_info["Number of Units and Stories"] = current_misc_propinfo_text
                elif misc_propinfo_el_index == 2:
                    general_property_info["Status"] = current_misc_propinfo_text
                else:
                    pass
             
            # get floor plans
            #pprint.pprint(general_property_info)
            floorplan_els = tree.xpath("//div[@class='profileContent']/div/section[@id='availabilitySection']/div[@id='pricingView']/div[@data-tab-content-id='all']/div[contains(@class, 'pricingGridItem')]")
            for floorplan_el in floorplan_els:
                # find floorplan info first
                this_floorplan = {"Time of Scraping":input_scrapetime, "URL":input_url, "Item Type":"Floor Plan", "Address":main_address,
                                  "Year Built":general_property_info["Year Built"], "Number of Units and Stories":general_property_info["Number of Units and Stories"],
                                  "Status":general_property_info["Status"]}
                this_floorplan["Floor Plan"] = ''
                this_floorplan["Bed"] = ''
                this_floorplan["Bath"] = ''
                this_floorplan["SF"] = ''
                this_floorplan["Price"] = ''
                this_floorplan["Available"] = ''

                planname_el = floorplan_el.xpath(".//h3[@class='modelLabel']/span[@class='modelName']")
                if len(planname_el) != 0:
                    this_floorplan["Floor Plan"] = planname_el[0].text_content().strip()

                planprice_el = floorplan_el.xpath(".//h3[@class='modelLabel']/span[@class='rentLabel']")
                if len(planprice_el) != 0:
                    this_floorplan["Price"] = planprice_el[0].text_content().replace('–', '-').strip()

                planavailable_el = floorplan_el.xpath(".//h4[@class='detailsLabel']//span[@class='availabilityInfo']")
                if len(planavailable_el) != 0:
                    this_floorplan["Available"] = planavailable_el[0].text_content().strip()
                    if this_floorplan["Available"].startswith("Available"):
                        this_floorplan["Available"] = this_floorplan["Available"][len("Available") : ].strip()


                misc_info_els = floorplan_el.xpath(".//h4[@class='detailsLabel']/span[@class='detailsTextWrapper']/span")
                for misc_info_el in misc_info_els:
                    cur_misc_text = misc_info_el.text_content().strip().lower()
                    cur_misc_number = None
                    key_to_change = None
                    
                    if cur_misc_text.endswith("bed") or cur_misc_text.endswith("beds"):
                        cur_misc_number = cur_misc_text[0:cur_misc_text.find("bed")].strip()
                        key_to_change = 'Bed'
                    elif cur_misc_text.endswith("bath") or cur_misc_text.endswith("baths"):
                        cur_misc_number = cur_misc_text[0:cur_misc_text.find("bath")].strip()
                        key_to_change = 'Bath'
                    elif cur_misc_text.endswith("sq ft"):
                        cur_misc_number = cur_misc_text[0:cur_misc_text.find("sq ft")].strip()
                        key_to_change = 'SF'
                    elif cur_misc_text == 'studio': # no number here!
                        this_floorplan["Bed"] = cur_misc_text.capitalize()
                    else:
                        pass

                    if key_to_change != None:
                        try:
                            if "." in cur_misc_number: # a float
                                this_floorplan[key_to_change] = float(cur_misc_number.replace(",", ""))
                            else: # an integer
                                this_floorplan[key_to_change] = int(cur_misc_number.replace(",", ""))
                        except (ValueError, TypeError):
                            if key_to_change == 'SF': # put original value!
                                this_floorplan[key_to_change] = cur_misc_number.replace("–", "-").replace(",", "").strip()

                            
                # next, find each unit and get its data!
                unit_els = floorplan_el.xpath("./div[contains(@class, 'unitGridContainer')]/div/ul/li[contains(@class, 'unitContainer')]")
                total_units_added_under_this_floorplan = 0
                for unit_el in unit_els:
                    this_unit = {"Item Type":"Unit", "Floor Plan":this_floorplan["Floor Plan"], "Bed":this_floorplan["Bed"], "Bath":this_floorplan["Bath"],
                                 "Time of Scraping":this_floorplan["Time of Scraping"], "Address":main_address, "Move-in Special":move_in_special,
                                 "Year Built":general_property_info["Year Built"], "Number of Units and Stories":general_property_info["Number of Units and Stories"], "Status":general_property_info["Status"]}
                    this_unit["Price"] = ''
                    this_unit["SF"] = ''
                    this_unit["Available"] = ''
                    this_unit["Unit"] = ''

                    price_el = unit_el.xpath(".//div[contains(@class, 'pricingColumn')]/span[@class='screenReaderOnly']/following-sibling::span[1]")
                    if len(price_el) != 0:
                        try:
                            this_unit["Price"] = float(price_el[0].text_content().replace("$", "").replace(",", "").strip())
                        except (ValueError, TypeError):
                            pass

                    sf_el = unit_el.xpath(".//div[contains(@class, 'sqftColumn')]/span[@class='screenReaderOnly']/following-sibling::span[1]")
                    if len(sf_el) != 0:
                        try:
                            this_unit["SF"] = float(sf_el[0].text_content().replace(",", "").strip())
                        except (ValueError, TypeError):
                            pass

                    avail_el = unit_el.xpath(".//div[contains(@class, 'availableColumn')]//span[@class='dateAvailable']/span[@class='screenReaderOnly']/following-sibling::text()[1]")
                    if len(avail_el) != 0:
                        this_unit["Available"] = avail_el[0].strip()

                    unit_number_el = unit_el.xpath(".//div[contains(@class, 'unitColumn')]//span[@class='screenReaderOnly']/following-sibling::span[@title]")
                    if len(unit_number_el) != 0:
                        this_unit["Unit"] = unit_number_el[0].text_content().strip()

                    # add if good
                    #pprint.pprint(this_unit)
                    if this_unit["Unit"] != "" and this_unit["Price"] != "" and this_unit["Floor Plan"] != "" and this_unit["Bed"] != "" and this_unit["Bath"] != "":
                        data_to_return.append(this_unit)
                        total_units_added_under_this_floorplan+=1
                    else:
                        print("Couldn't add a unit because some info is missing at", input_url)

                # add the floorplan if no units added
                if total_units_added_under_this_floorplan == 0:
                    data_to_return.append(this_floorplan)

                
        if len(data_to_return) == 0:
            print("No units or floorplans found at", input_url)
        return data_to_return


    def fix_string(self, entry_string): # remove "\n", "\t" and double spaces
        exit_string = entry_string.replace("\n", "")
        exit_string = exit_string.replace("\t", "")
        exit_string = exit_string.replace("\r", "")
        while "  " in exit_string:
            exit_string = exit_string.replace("  ", " ")
        if len(exit_string) > 0: # remove first space
            if exit_string[0] == ' ':
                exit_string = exit_string[1:len(exit_string)]
        if len(exit_string) > 0: # remove last space
            if exit_string[len(exit_string)-1] == ' ':
                exit_string = exit_string[0:len(exit_string)-1]

        return exit_string


if __name__ == '__main__':
    pars = ApartmentsParser(INPUT_FILE, DATABASE_NAME, BATCH_SIZE, mode='write')
    pars.write_data()
