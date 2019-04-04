"""
Scrape daily Atlanta pollen count data from the Atlanta Allergy and Asthma
website, then write to a text file
"""


# import libraries
import datetime  # date handling

dt = datetime.date
import requests  # http requests
from bs4 import BeautifulSoup  # html parsing
import re  # regex parsing
import pandas as pd


# base url (date is appended in loop)
base_url = (
    'http://www.atlantaallergy.com/pollen_counts/index/'
)


# input date range - starts 6/18/1991
start_date = '2012-01-01'
end_date = '2012-12-31'


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
contributor_severities = []

severity_dates = []
severity_categories = []


# loop for each date in range
while current_date <= end_date:
    print('Starting ' + str(current_date))

    # send the get request with the current date
    # the user agent is set to avoid blocking; there is no specific reason for
    # using 'custom' (it could be any string)
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
    else:
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
        else:
            daily_count = daily_count.text.strip()

            # append the daily results
            result_dates.append(current_date)
            result_counts.append(daily_count)

            # get the detailed contributors
            for g in soup.find_all(class_='gauge'):
                # get the contributor (trees, weeds, etc.)
                contributor_type = (
                    g.h5.text
                )  # return text in h5 tag
                contributor_type = re.match(
                    '^(\w)+', contributor_type
                )[
                    0
                ]  # first word
                #
                # get the list of types (sycamore, etc.)
                contributor_name = g.p.text.strip()

                # get the severity value (0 - 100%)
                contributor_severity = g.find(
                    class_='needle'
                )['style']
                contributor_severity = re.match(
                    '.*?([\d\.+]+)%', contributor_severity
                )[1]

                # add results to list
                contributor_dates.append(current_date)
                contributor_types.append(contributor_type)
                contributor_names.append(contributor_name)
                contributor_severities.append(
                    contributor_severity
                )
        # if it is the last day of the month, or the last day in the range,
        # scrape the severity level from the calendar
        if (
            current_date.month
            != (
                current_date + datetime.timedelta(days=1)
            ).month
            or current_date == end_date
        ):

            for d in soup.find_all(
                class_=re.compile('calendar-day current .*')
            ):
                severity_category = re.search(
                    '.*?calendar-day current (.*?)\\">',
                    str(d),
                )[1]
                severity_date = datetime.date(
                    current_date.year,
                    current_date.month,
                    int(
                        d.find(
                            class_='day-num'
                        ).text.strip()
                    ),
                )

                # append results
                severity_dates.append(severity_date)
                severity_categories.append(
                    severity_category
                )
        # increment the date
        current_date += datetime.timedelta(days=1)
# end of while loop


# print result counts
total_days = abs(end_date - start_date).days + 1
success_days = len(result_dates)
error_days = len(error_list)
print(
    '\nSuccessfully processed '
    + str(success_days)
    + ' of '
    + str(total_days)
    + ' days.\n'
)

# print errors
if len(error_list) > 0:
    print('The following errors occurred:')
    for i in error_list:
        print(
            str(i['error_date'])[0:10]
            + ' --- '
            + i['error_type']
        )
# convert to data frames
pollen_count_df = pd.DataFrame(
    {'date': result_dates, 'pollen_count': result_counts}
)
pollen_contributors_df = pd.DataFrame(
    {
        'date': contributor_dates,
        'contributor_type': contributor_types,
        'contributor_names': contributor_names,
        'contributor_severity': contributor_severities,
    }
)
pollen_severity_df = pd.DataFrame(
    {
        'date': severity_dates,
        'severity_category': severity_categories,
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
pollen_count_df.to_csv(
    '.\data\pollen_count_'
    + dt.isoformat(start_date)
    + '_to_'
    + dt.isoformat(end_date)
    + '.csv',
    index=False,
)
pollen_contributors_df.to_csv(
    '.\data\pollen_count_contributors_'
    + dt.isoformat(start_date)
    + '_to_'
    + dt.isoformat(end_date)
    + '.csv',
    index=False,
)