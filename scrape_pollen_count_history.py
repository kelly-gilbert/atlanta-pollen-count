#------------------------------------------------------------------------------
#  
#------------------------------------------------------------------------------

# import the libraries
import datetime                  # date handling
dt = datetime.datetime

import requests                  # http requests
from bs4 import BeautifulSoup    # html parsing
import re                        # regex parsing


# base url; the date is added to the end in YYYY/MM/DD format
base_url = 'http://www.atlantaallergy.com/pollen_counts/index/'

# input date range to process
start_date = '2019-03-01'
end_date = '2019-03-02'


# initialize the date
current_date = dt.strptime(start_date, '%Y-%m-%d')
end_date = dt.strptime(end_date, '%Y-%m-%d')    # convert to datetime

# initialize the lists
error_dates = []
error_types = []
results = []


while current_date <= end_date:

    # send the get request with the current date
    # the user agent is set to avoid blocking; there is no specific reason for
    # using 'custom' (it could be any string)
    r = requests.get(base_url + dt.strftime(current_date, '%Y/%m/%d'), 
                     headers={'User-Agent': 'Custom'})

    # if the request was not successful, log an error
    if r.status_code != 200:
        error_dates.append(start_date)
        error_types.append('Request status: ' + str(r.status_code) + ' - ' 
                           + r.reason) 
    else:
        # convert result text to a soup object
        soup = BeautifulSoup(r.text, 'lxml')

        # get the overall count
        daily_count = soup.body.find(class_='pollen-num')
        daily_count = daily_count.text.strip()
        print(daily_count)
        
        # get the contributors
        category_list = []
        for g in soup.find_all(class_='gauge'):         
            # get the category (trees, weeds, etc.)
            category_name = g.h5.text    # return text in h5 tag
            category_name = re.match('^(\w)+', category_name)[0]    # first word
#            print(category_name)

            # get the list of types (sycamore, etc.)
            category_types = g.p.text.strip()
#            print(category_types)

            # get the severity value (0 - 100%)          
            category_amount = g.find(class_='needle')['style']
            category_amount = re.match('.*?([\d\.+]+)%', category_amount)[1]
#            print(category_amount)
 
            # create a dictionary of detailed results
            category_dict = { 'category_name' : category_name,
                              'category_types' : category_types, 
                              'category_amount' : category_amount }
            category_list.append(category_dict)
#            print(category_list)
        
        # create a dictionary of result details
        results_dict = { 'date' : current_date,
                         'daily_count' : daily_count, 
                         'category_list' : category_list }
        
    
        # add the daily results to the results list
        results.append(results_dict)

        # increment the date
        current_date += datetime.timedelta(days=1)
#        print(current_date)

print(error_dates)
print(error_types)
for i in results:
    print(i)