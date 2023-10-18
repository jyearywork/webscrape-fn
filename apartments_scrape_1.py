# install: requests, lxml
import requests
from lxml import html
import csv
import threading
import bz2
import sqlite3
from datetime import datetime
import logging

# INPUTS
INPUT_FILE = "./data/apt_comps.csv"
DATABASE_NAME = "./data/ApartmentscomDatabase.db"
BATCH_SIZE = 1 # how many parallel requests
# END OF INPUTS


class ApartmentsScraper:
    def __init__(self, input_filename, input_databasename, input_batchsize, mode='scrape'):  # noqa: E501
        ## check if inputs are good
        self.inputs_are_good = True
        self.is_interrupted = False

        input_checks = [#self.check_input('INPUT_FILE', 'str', input_filename),
                        self.check_input('DATABASE_NAME', 'str', input_databasename),
                        self.check_input('BATCH_SIZE', 'positive_int', input_batchsize)
                        ]
        if False in input_checks:
            logging.info("Bad inputs, quit!")
            self.inputs_are_good = False
            return

        ## if still here, set inputs
        self.input_file = input_filename
        self.database_name = input_databasename
        self.batch_size = input_batchsize

        ## create database
        self.db_conn = sqlite3.connect(self.database_name, check_same_thread=False)
        self.db_cursor = self.db_conn.cursor()
        self.db_cursor.execute("CREATE TABLE IF NOT EXISTS DataTable (link TEXT NOT NULL PRIMARY KEY, html BLOB, time_of_scraping TEXT, timestamp REAL)")  # noqa: E501

        ## set items for threading
        self.good_count = 0
        self.LOCK = threading.Lock()

        ## read inputs, only if scraping
        if mode == 'scrape':
            self.items_to_scrape = self.read_inputs()
            
        return


    def read_inputs(self, header_to_look_for='link'):
        try:
            items_to_return = []
            #with open(self.input_file, 'r', encoding='utf-8') as f:
            f = self.input_file
            reader = csv.reader(f, delimiter=",")
            
            is_header_row = True
            link_index = None
            
            for line in reader:
                if is_header_row == True:  # noqa: E712
                    if header_to_look_for in line:
                        link_index = line.index(header_to_look_for)
                    is_header_row = False
                    continue

                if link_index == None:  # noqa: E711
                    logging.info("Can't find header in csv:", header_to_look_for)
                    break

                try:
                    link_to_add = line[link_index]
                    items_to_return.append({"link":link_to_add})
                except IndexError:
                    continue
                        
            
            return items_to_return
        except:  # noqa: E722
            logging.info("An exception while reading inputs - make sure input filename is correct!")  # noqa: E501
            return []



    def scrape(self):
        if self.inputs_are_good == False or self.is_interrupted == True:
            return

        logging.info("Scraping unscraped items...")
        self.good_count = 0
        all_thread_items = {}
        
        for item_to_scrape in self.items_to_scrape:
            existence_check = self.db_cursor.execute("SELECT EXISTS(SELECT 1 FROM DataTable WHERE link=?)", (item_to_scrape["link"], )).fetchone()[0]  # noqa: E501
            if existence_check == 1:
                continue # already scraped

            if item_to_scrape["link"] in all_thread_items:
                continue # already is inside

            # if here, must scrape this one
            all_thread_items[item_to_scrape["link"]] = {"link":item_to_scrape["link"]}
            if len(all_thread_items) == self.batch_size:
                ## call it
                all_threads = []
                for a_thread_item in all_thread_items:
                    current_thread = threading.Thread(target=self.apartment_thread, args=(all_thread_items[a_thread_item],) )  # noqa: E501
                    all_threads.append(current_thread)
                    current_thread.start()

                for thr in all_threads:
                    thr.join()

                logging.info("Current item", self.items_to_scrape.index(item_to_scrape)+1, "/", len(self.items_to_scrape), "Good requests in this batch:", self.good_count, "/", len(all_thread_items))  # noqa: E501
                self.good_count = 0
                all_thread_items = {}


        if len(all_thread_items) != 0:
            ## call for residuals
            all_threads = []
            for a_thread_item in all_thread_items:
                current_thread = threading.Thread(target=self.apartment_thread, args=(all_thread_items[a_thread_item],) )  # noqa: E501
                all_threads.append(current_thread)
                current_thread.start()

            for thr in all_threads:
                thr.join()

            logging.info("Current item", self.items_to_scrape.index(item_to_scrape)+1, "/", len(self.items_to_scrape), "Good requests in this batch:", self.good_count, "/", len(all_thread_items))  # noqa: E501
            self.good_count = 0
            all_thread_items = {}
            
        return


    def apartment_thread(self, input_dict):
        good_to_save = False
        try:
            r = requests.get(input_dict["link"], timeout=20,
                             headers={"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"})  # noqa: E501
            c = r.content
            tree = html.fromstring(c)
            verificator_el = tree.xpath("//header[@id='profileHeaderWrapper']//h1[@id='propertyName']")  # noqa: E501
            if len(verificator_el) != 0:
                good_to_save = True
        except:
            return

        if good_to_save == True:
            # save it
            current_time_object = datetime.now()
            with self.LOCK:
                try:
                    self.db_cursor.execute("INSERT INTO DataTable (link, html, time_of_scraping, timestamp) VALUES(?,?,?,?)",  # noqa: E501
                                           (input_dict["link"], bz2.compress(c), current_time_object.strftime("%d-%B-%Y"), current_time_object.timestamp() ))  # noqa: E501
                    self.db_conn.commit()
                    self.good_count+=1
                except:
                    pass
        return


    def check_input(self, input_name, input_type, input_value):
        input_is_good = True
        if input_type == 'str':
            if type(input_value) != str:
                input_is_good = False
                logging.info(input_name + " should be a string!")
        elif input_type == 'positive_int':
            if type(input_value) != int:
                input_is_good = False
                logging.info(input_name + " should be an integer!")
            else:
                if input_value <= 0:
                    input_is_good = False
                    logging.info(input_name + " should be a positive integer!")
        else:
            logging.info("Unhandled input type: " + input_type)
            
        return input_is_good


if __name__ == '__main__':
    scraper = ApartmentsScraper(INPUT_FILE, DATABASE_NAME, BATCH_SIZE)
    scraper.scrape()
