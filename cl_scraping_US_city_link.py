##########
# This code dowlonads the html file of the page:
# Extract city name and its link
# save it to city_link_result.txt file
#########


##################################################################################################
# import liraries
import re
import urllib.request
import urllib
from urllib.parse import urljoin,urlparse
from urllib import robotparser
from urllib.error import URLError, HTTPError, ContentTooShortError
from bs4 import BeautifulSoup
import lxml
import time
import csv
import sqlite3
##################################################################################################
cl_root_url= "https://www.craigslist.org/about/sites"
result_file_path = "state_city_link.csv"
database = 'cl_scraping_db.sqlite'
s_c_l_table = 'state_city_link_table'
##################################################################################################

# The donwload function
def download(url, user_agent='wswp', num_retries=2, charset='utf-8', proxy=None):
    print('Downloading:', url)
    request = urllib.request.Request(url)
    request.add_header('User-agent', user_agent)
    try:
        if proxy:
            proxy_support = urllib.request.ProxyHandler({'http': proxy})
            opener = urllib.request.build_opener(proxy_support)
            urllib.request.install_opener(opener)
        resp = urllib.request.urlopen(request)
        cs = resp.headers.get_content_charset()
        if not cs:
            cs = charset
        html = resp.read().decode(cs)
    except (URLError, HTTPError, ContentTooShortError) as e:
        print('Download error:', e.reason)
        html = None
        if num_retries > 0:
            if hasattr(e, 'code') and 500 <= e.code < 600:
                # recursively retry 5xx HTTP errors
                return download(url, num_retries - 1)
    return html

#Create the state_city_link table in our database if does not exit
def create_state_city_link_table_and_col(database, s_c_l_table):
    # Create a database where "state_city_link.sqlite"
    conn = sqlite3.connect(database)
    c = conn.cursor()
    #Create the table if not exist
    c.execute('CREATE TABLE IF NOT EXISTS {tn} ({nf} {ft})'.format(tn=s_c_l_table, nf="state_col", ft='TEXT'))
    # Create corresponding columns
    try:
        c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=s_c_l_table, cn="city_col", ct="TEXT"))
    except sqlite3.OperationalError:
        print ('col exist')
    try:
        c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=s_c_l_table, cn="link_col", ct="TEXT"))
    except sqlite3.OperationalError: 
        print ('col exist')
    # Commit changes
    conn.commit()
    conn.close()


# Create the result and store it inside a file and the database
def download_state_city_link_to_file_and_db(url,file_path):
    ##################################################################################################
    # Donload the html file of the root to extract State, city, CL link
    html = download(url)
    # Parse the downloaded document to a beautiful soup document
    soupe = BeautifulSoup(html, "lxml")
    # Extract USA divs
    divs = soupe.find_all('div', {'class':"colmask"})
    us_div = divs[0]
    # Get the ul list that contains the city_url informatoion of each state
    uls = us_div.find_all('ul')
    # Get the states list 
    states = us_div.find_all('h4')
    # zip the two list together to 
    states_cities_zipped = list(zip(states, uls))
    # write the result State, city, link to reslut.txt file
    f=open(file_path, 'w')
    #connect to the database
    conn = sqlite3.connect(database)
    c = conn.cursor()


    # loop over the zipped list and extract the state's name and a city_link pair within that state
    for ele in states_cities_zipped:
        state = ele[0].string
        # get the urls 
        a_urls = ele[1].find_all('a')
        # Loop over the a_urls within the state
        for url in a_urls:
            link = url.get('href')
            city = url.string
            #write to the file
            f.write(state+","+city+","+link+"\n")
            # Write to the database
            c.execute("INSERT INTO state_city_link_table (state_col, city_col, link_col) VALUES (?, ?,  ?)",(state, city, link))
            print (state+","+city+","+link+"\n")
    f.close()
    # Commit and close the database connection
    conn.commit()
    conn.close()




create_state_city_link_table_and_col(database, s_c_l_table)

download_state_city_link_to_file_and_db(cl_root_url,  result_file_path)
























