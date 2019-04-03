"""
Scrape daily Atlanta pollen count data from the Atlanta Allergy and Asthma
website, then write to a text file
"""


# import the libraries
import datetime                  # date handling
dt = datetime.datetime

import requests                  # http requests
from bs4 import BeautifulSoup    # html parsing
import re                        # regex parsing

import pandas as pd


# base url (date is appended in loop)
base_url = 'http://www.atlantaallergy.com/pollen_counts/index/'

# input date range
start_date = '2019-03-01'
end_date = '2019-03-15'


# initialize the dates
start_date = dt.strptime(start_date, '%Y-%m-%d')
end_date = dt.strptime(end_date, '%Y-%m-%d')
current_date = start_date

# initialize the lists
error_list = []

result_dates = []
result_counts = []

contributor_dates = []
contributor_types = []
contributor_names = []
contributor_severities = []

severity_dates = []
severity_categories = []


while current_date <= end_date:
    print('Starting ' + str(current_date))
  
    # send the get request with the current date
    # the user agent is set to avoid blocking; there is no specific reason for
    # using 'custom' (it could be any string)
    r = requests.get(base_url + dt.strftime(current_date, '%Y/%m/%d'),
                     headers={'User-Agent': 'Custom'})

    # if the request was not successful, log an error
    if r.status_code != 200:
        error_list.append( { 'error_date' : current_date,
                             'error_type' : 'Request status: ' + str(r.status_code) \
                             + ' - ' + r.reason } )
    else:
        # convert result text to a soup object
        soup = BeautifulSoup(r.text, 'lxml')

        # get the overall count
        daily_count = soup.body.find(class_='pollen-num')
        if not daily_count:    # no pollen count available
            error_list.append( { 'error_date' : current_date,
                                 'error_type' : 'Pollen count not available' } )                        
        else:
            daily_count = daily_count.text.strip()

            # append the daily results
            result_dates.append(current_date)
            result_counts.append(daily_count)
        
            # get the detailed contributors
            for g in soup.find_all(class_='gauge'):
                # get the contributor (trees, weeds, etc.)
                contributor_type = g.h5.text    # return text in h5 tag
                contributor_type = re.match('^(\w)+', contributor_type)[0]    # first word
#
                # get the list of types (sycamore, etc.)
                contributor_name = g.p.text.strip()

                # get the severity value (0 - 100%)          
                contributor_severity = g.find(class_='needle')['style']
                contributor_severity = re.match('.*?([\d\.+]+)%', contributor_severity)[1]
 
               # add results to list
                contributor_dates.append(current_date)
                contributor_types.append(contributor_type)
                contributor_names.append(contributor_names)
                contributor_severities.append(contributor_severity)

 
        # if it is the last day of the month, or the last day in the range,
        # scrape the severity level from the calendar
        if current_date.month != (current_date + datetime.timedelta(days=1)).month  \
           or current_date == end_date:
 
            for d in soup.find_all(class_=re.compile('calendar-day current .*')):
                severity_category = re.search('.*?calendar-day current (.*?)\\">', str(d))[1]
                severity_date = datetime.date(current_date.year, current_date.month, 
                                              int(d.find(class_='day-num').text.strip()))
 
                # append results
                severity_dates.append(severity_date)
                severity_categories.append(severity_category)

        # increment the date
        current_date += datetime.timedelta(days=1)
        
# end of while loop
        

# print result counts
total_days = abs(end_date - start_date).days + 1
success_days = len(result_dates)
error_days = len(error_list)
print('Successfully processed ' + str(success_days) + ' of ' 
      + str(total_days) + ' days.\n')

# print errors
if len(error_list) > 0:
    print('The following errors occurred:')
    for i in error_list:
        print(str(i['error_date'])[0:10] + ' --- ' + i['error_type'])
        

# convert to data frames
pollen_count_df = pd.DataFrame( { 'date' : result_dates, 'pollen_count' : result_counts })
pollen_contributors_df = pd.DataFrame( { 'date' : contributor_dates, 'contributor_type' : contributor_types, 'contributor_names' : contributor_names, 'contributor_severity' : contributor_severities})
pollen_severity_df = pd.Data.Frame( { 'date' : severity_dates, 'severity_category' : severity_categories })

pollen_count_df.head(10)
pollen_contributors_df.head(10)
pollen_severity_df.head(10)

# results for debugging:
#print(error_list)
#for i in results:
#    print(i)
    
# write results to file
    
pollen_count_df.to_csv('pollen_count_' + dt.strftime(start_date, '%Y-%m-%d') + '_to_' + dt.strftime(end_date, '%Y-%m-%d') +'.csv')
    
    
    
    