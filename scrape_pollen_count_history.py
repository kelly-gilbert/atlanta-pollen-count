""" Scrape daily Atlanta pollen count data from the Atlanta Allergy and Asthma
website, then write to a text file """


# import the libraries
import datetime                  # date handling
dt = datetime.datetime

import requests                  # http requests
from bs4 import BeautifulSoup    # html parsing
import re                        # regex parsing


# base url (date is appended in loop)
base_url = 'http://www.atlantaallergy.com/pollen_counts/index/'

# input date range
start_date = '2019-03-01'
end_date = '2019-03-02'


# initialize the dates
start_date = dt.strptime(start_date, '%Y-%m-%d')
end_date = dt.strptime(end_date, '%Y-%m-%d')
current_date = start_date

# initialize the lists
error_list = []
results_list = []

while current_date <= end_date:
    #print('Starting ' + str(current_date))
        
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
#           print(daily_count)
        
            # get the contributors
            contributor_list = []
            for g in soup.find_all(class_='gauge'):         
                # get the contributor (trees, weeds, etc.)
                contributor_name = g.h5.text    # return text in h5 tag
                contributor_name = re.match('^(\w)+', contributor_name)[0]    # first word
#                print(contributor_name)

                # get the list of types (sycamore, etc.)
                contributor_types = g.p.text.strip()
#                print(contributor_types)

                # get the severity value (0 - 100%)          
                contributor_amount = g.find(class_='needle')['style']
                contributor_amount = re.match('.*?([\d\.+]+)%', contributor_amount)[1]
#                print(contributor_amount)
                
                # create a dictionary of detailed results
                contributor_dict = { 'contributor_name' : contributor_name,
                                 'contributor_types' : contributor_types, 
                                 'contributor_amount' : contributor_amount }
                contributor_list.append(contributor_dict)
                # print(contributor_list)
                
            # create a dictionary of result details for the current date
            results_dict = { 'date' : current_date,
                            'daily_count' : daily_count, 
                            'contributor_list' : contributor_list }
                
                
            # add the daily results to the results list
            results_list.append(results_dict)
        
        
        # if it is the last day of the month, or the last day in the range,
        # scrape the severity level from the calendar
        if current_date.month < (current_date + datetime.timedelta(days=1)).month
           or current_date = end_date:
            
            for d in soup.find_all(class_=re.compile('calendar-day current.*'))[0:2]:
                print(d)
                severity_dict = {}
                severity_dict['severity'] = re.match('.*?calendar-day current (.*?)\\">', str(d))[1] 
                print(severity_dict['severity'])
                
                severity_dict['severity_date'] = datetime.date(current_date.year, current_date.month, 
                                     int(d.find(class_='day-num').text.strip()))
                print(severity_dict['severity_date'])
            print(severity_dict)
          





        
        # increment the date
        current_date += datetime.timedelta(days=1)
        

# end of while loop
        

# print result count
total_days = abs(end_date - start_date).days + 1
success_days = len(results_list)
error_days = len(error_list)
print('Successfully processed ' + str(success_days) + ' of ' 
      + str(total_days) + ' days.\n')

# print errors
if len(error_list) > 0:
    print('The following errors occurred:')
    for i in error_list:
        print(str(i['error_date'][0:10]) + ' --- ' + i['error_type'])



# results for debugging:
#print(error_list)
#for i in results:
#    print(i)
    
# write results to file
    
    
    
    
    
    