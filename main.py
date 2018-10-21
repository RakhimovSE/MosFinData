import csv
import datetime
from datetime import datetime
import json

import reformagkh


def log(msg, account=None):
    date_str = datetime.datetime.now().strftime(reformagkh.DATETIME_FORMAT)
    if account:
        print('[%s] %s (%s): %s' % (date_str, account['data']['name'], account['data']['facebook_login'], msg))
    else:
        print('[%s] %s' % (date_str, msg))


def write_csv(i, data):
    with open('coinmarketcap.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow((data['name'],
                         data['price']))
        print(i, data['name'], 'parsed')


if __name__ == '__main__':
    reformagkh.test()