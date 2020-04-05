"""
Scrape daily Atlanta pollen count data from the Atlanta Allergy and Asthma
website (http://www.atlantaallergy.com/pollen_counts), 
then write to a text file
"""


# import libraries
import datetime  # date handling
dt = datetime.date
import requests  # http requests
from bs4 import BeautifulSoup  # html parsing
import re  # regex parsing
import pandas as pd
import os


# define functions
def increment_date(in_date, num_days):
    """ increment the date by num_days """
    in_date += datetime.timedelta(days=1)
    return in_date


def is_end_of_month(in_date):
    """ return True if in_date is the end of the month """
    if in_date.month != (in_date + datetime.timedelta(days=1)).month:
        return True
    else:
        return False
    

def get_severity_levels(current_date, soup, severity_dates, severity_levels):
    """
    if it is the last day of the month, or the last day in the range,
    scrape the severity level from the calendar at the bottom of the page
    """

    for d in soup.find_all(class_=re.compile('calendar-day current .*')):
        severity_level = re.search(
            '.*?calendar-day current (.*?)\\">',
            str(d),)[1]
        severity_date = datetime.date(
            current_date.year,
            current_date.month,
            int(d.find(class_='day-num').text.strip()),
        )

        # append results
        severity_dates.append(severity_date)
        severity_levels.append(severity_level)


def get_pollen_counts(start_date, end_date):
    """ 
    retrieve the pollen count data for the specified date range
    and write it to a csv file 
    """
    
    # base url (date is appended in loop)
    base_url = 'http://www.atlantaallergy.com/pollen_counts/index/'
 
    # initialize dates as dates
    start_date = dt.fromisoformat(start_date)
    end_date = dt.fromisoformat(end_date)
    current_date = start_date
    
    # initialize result lists
    error_list = []
    
    result_dates = []
    result_counts = []
    
    contributor_dates = []
    contributor_types = []
    contributor_names = []
    contributor_severity_pcts = []
    contributor_severity_labels = []
    
    severity_dates = []
    severity_levels = []

    
    # loop for each date in range
    print('Starting loop : ' + str(start_date) + '...')
    
    while current_date <= end_date:  
        # send the get request with the current date
        # the user agent is set to avoid blocking; there is no specific reason 
        # for using 'custom' (it could be any string)
        r = requests.get(
            base_url + dt.strftime(current_date, '%Y/%m/%d'),
            headers={'User-Agent': 'Custom'},
        )

    
        # if the request was not successful, log an error
        if r.status_code != 200:
            error_list.append(
                {
                    'error_date': current_date,
                    'error_type': 'Request status: '
                        + str(r.status_code)
                        + ' - '
                        + r.reason,
                }
            )
            
            # go to the next date
            current_date = increment_date(current_date, 1)
            continue
        
        # convert result text to a soup object
        soup = BeautifulSoup(r.text, 'lxml')
    
        # get the overall count
        daily_count = soup.body.find(class_='pollen-num')
        if not daily_count:  # no pollen count available
            error_list.append(
                {
                    'error_date': current_date,
                    'error_type': 'Pollen count not available',
                }
            )
            
            # get severity levels (if end of month or end of range)
            # the end of month may fall on a weekend with no count data
            if is_end_of_month(current_date) or current_date == end_date:
                get_severity_levels(
                    current_date, 
                    soup, 
                    severity_dates, 
                    severity_levels
                )

            # go to the next date
            current_date = increment_date(current_date, 1)                  
            continue
       
        
        # if the count exists, append it to the daily results list
        result_dates.append(current_date)
        result_counts.append(daily_count.text.strip())

        # get the detailed contributors
        for gauge in soup.find_all(class_='gauge'):
            # get the contributor (trees, weeds, etc.) in h5 tag (or strong tag for mold)
            if gauge.h5 == None:
              contributor_type = 'Mold'
            else:    
              contributor_type = re.match('^(\w)+', gauge.h5.text)[0]
            
            # get the list of types (sycamore, etc.)
            if contributor_type == 'Mold':
              contributor_name = 'Mold'
            else: 
              contributor_name = gauge.p.text.strip()
    
            # get the severity value (0 - 99)
            contributor_severity_pct = gauge.find(class_='needle')['style']
            contributor_severity_pct = re.match('.*?([\d\.+]+)%', 
                                            contributor_severity_pct)[1]

            # get the severity value (0 - 99)
            contributor_severity_label = gauge.find(
                    class_=re.compile('.*? active')
                ).text
    
            # add results to lists
            contributor_dates.append(current_date)
            contributor_types.append(contributor_type)
            contributor_names.append(contributor_name)
            contributor_severity_pcts.append(contributor_severity_pct)
            contributor_severity_labels.append(
                contributor_severity_label
            )
    
    
        # get severity levels
        if is_end_of_month(current_date) or current_date == end_date:
            get_severity_levels(
                current_date, 
                soup, 
                severity_dates, 
                severity_levels
            )

        
        # increment the date
        current_date = increment_date(current_date, 1)
    
    # end of while loop
    
    
    # print result counts
    total_days = abs(end_date - start_date).days + 1
    success_days = len(result_dates)
    print(
        'Successfully processed ' + str(success_days) + ' of '
        + str(total_days) + ' days.'
    )
    
    # print errors
    #if len(error_list) > 0:
    #    print('The following errors occurred:')
    #    for i in error_list:
    #        print(
    #            str(i['error_date'])[0:10]
    #            + ' --- '
    #            + i['error_type']
    #        )
    
    
    # convert to data frames
    print('Creating data frames...')
    
    pollen_count_df = pd.DataFrame(
        {'date': result_dates, 'pollen_count': result_counts}
    )
    pollen_contributors_df = pd.DataFrame(
        {
            'date': contributor_dates,
            'contributor_type': contributor_types,
            'contributor_names': contributor_names,
            'contributor_severity_pct': contributor_severity_pcts,
            'contributor_severity_label' : contributor_severity_labels,
        }
    )
    pollen_severity_df = pd.DataFrame(
        {
            'date': severity_dates,
            'severity_level': severity_levels,
        }
    )
        
    
    # merge the daily severity (high/med/low) to the main dataframe
    pollen_count_df2 = pd.merge(
        pollen_count_df,
        pollen_severity_df,
        how='left',
        on='date',
        left_index=False,
        right_index=False,
        sort=False,
        validate='1:m',
    )
    
    
    # write results to files
    print('Writing results to files...')
    
    # if full year, name the file with the year, otherwise use the date range
    if start_date.month == 1 and start_date.day == 1  \
       and end_date.month == 12 and end_date.day == 31:
           file_date = str(end_date.year)
    else:
       file_date = dt.isoformat(start_date) + '_to_' + dt.isoformat(end_date)
       
    # get the path   
    file_path = os.path.dirname(os.getcwd()).replace('\\', '\\\\')    # parent directory

    pollen_count_df2.to_csv(
        file_path + '\\data\\pollen_count_' + file_date + '.csv',
        index=False,
    )
    
    pollen_contributors_df.to_csv(
        file_path + '\\data\\pollen_count_contributors_' + file_date + '.csv',
        index=False,
    )
    
    print('Done\n\n')
    
    
# get data for each year  
get_pollen_counts('2019-01-01', '2019-12-31')
#get_pollen_counts('2018-01-01', '2018-12-31')
#get_pollen_counts('2017-01-01', '2017-12-31')
#
#get_pollen_counts('2016-01-01', '2016-12-31')
#get_pollen_counts('2015-01-01', '2015-12-31')
#get_pollen_counts('2014-01-01', '2014-12-31')
#get_pollen_counts('2013-01-01', '2013-12-31')
#get_pollen_counts('2012-01-01', '2012-12-31')
#
#get_pollen_counts('2011-01-01', '2011-12-31')
#get_pollen_counts('2010-01-01', '2010-12-31')
#get_pollen_counts('2009-01-01', '2009-12-31')
#get_pollen_counts('2008-01-01', '2008-12-31')
#get_pollen_counts('2007-01-01', '2007-12-31')
#
#get_pollen_counts('2006-01-01', '2006-12-31')
#get_pollen_counts('2005-01-01', '2005-12-31')
#get_pollen_counts('2004-01-01', '2004-12-31')
#get_pollen_counts('2003-01-01', '2003-12-31')
#get_pollen_counts('2002-01-01', '2002-12-31')
#
#get_pollen_counts('2001-01-01', '2001-12-31')
#get_pollen_counts('2000-01-01', '2000-12-31')
#get_pollen_counts('1999-01-01', '1999-12-31')
#get_pollen_counts('1998-01-01', '1998-12-31')
#get_pollen_counts('1997-01-01', '1997-12-31')
