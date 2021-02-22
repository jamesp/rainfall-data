import csv
import json
import os
import optparse
import urllib.request
import datetime

DEFAULT_DATE = (datetime.date.today() - datetime.timedelta(days=2)).isoformat()

parser = optparse.OptionParser()

parser.add_option('-s', '--stations', default='./stations.txt')
parser.add_option('-d', '--date', default=DEFAULT_DATE)

options, args = parser.parse_args()

def get_stations(filename='stations.txt'):
    with open(filename, 'r') as f:
        stations = json.load(f)
    return stations

stations = get_stations(options.stations)

date = options.date
url = f"http://environment.data.gov.uk/flood-monitoring/archive/readings-{date}.csv"

resp = urllib.request.urlopen(url)
dd = csv.DictReader((l.decode('utf-8') for l in resp.readlines()), )

data = {s['location']: [] for s in stations}

for row in dd:
    for station in stations:
        loc = station['location']
        sid = station['station_id']
        if sid in row['measure']:
            data[loc].append({'timestamp': row['dateTime'], 'value': float(row['value'])})

for location, points in data.items():
    year = date.split('-')[0]
    datadir = f'data/{location}/{year}'
    if not os.path.isdir(datadir):
        os.makedirs(datadir)
    out = {
        'location': location,
        'date': date,
        'data': points
    }
    with open(f'{datadir}/{date}.json', 'w') as f:
        json.dump(out, f, indent=2)