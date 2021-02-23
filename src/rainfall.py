import csv
import datetime
import itertools
import json
import os
import optparse
import urllib.request


TESTING = True
DEFAULT_DATE = (datetime.date.today() - datetime.timedelta(days=2)).isoformat()

parser = optparse.OptionParser()

parser.add_option('-s', '--stations', default='./stations.txt')
parser.add_option('-d', '--date', default=DEFAULT_DATE)
parser.add_option('-f', '--file')

options, args = parser.parse_args()

def get_stations(filename='stations.txt'):
    with open(filename, 'r') as f:
        stations = json.load(f)
    return stations

stations = get_stations(options.stations)

date = options.date
year, month, day = date.split('-')
today = datetime.datetime(int(year), int(month), int(day))

if options.file:
    resp = open(options.file, 'rb')
else:
    url = f"http://environment.data.gov.uk/flood-monitoring/archive/readings-{date}.csv"
    resp = urllib.request.urlopen(url)

dd = csv.DictReader((l.decode('utf-8') for l in resp.readlines()), )

data = {s['location']: [] for s in stations}
for row in dd:
    for station in stations:
        loc = station['location']
        sid = station['station_id']
        if sid in row['measure']:
            timestamp = datetime.datetime.strptime(row['dateTime'], '%Y-%m-%dT%H:%M:%SZ')
            data[loc].append({'timestamp': timestamp, 'value': float(row['value'])})

# group by hour
hourly = {}
sortkey = lambda x: x['timestamp'].hour
for location, points in data.items():
    points = sorted(points, key=sortkey)
    grouped = itertools.groupby(points, sortkey)
    hours = ((key, sum(point['value'] for point in group)) for key, group in grouped)
    hours = [(today + datetime.timedelta(hours=hour), value) for hour, value in hours]
    hourly[location] = hours

for location in hourly:
    hourly_file = f'data/{location}/{year}-{month}.csv'
    if os.path.exists(hourly_file):
        with open(hourly_file, 'r') as f:
            r = csv.reader(f)
            existing_data = dict(list(r))
    else:
        existing_data = {}
    
    points = hourly[location]
    for time, value in points:
        existing_data[time.isoformat()] = value

    with open(hourly_file, 'w') as f:
        writer = csv.writer(f)
        writer.writerows((d, existing_data[d]) for d in sorted(existing_data))


# group by day
daily = {location: (today.strftime('%Y-%m-%d'), sum(v[1] for v in values)) for location, values in hourly.items()}
for location in daily:
    daily_file = f'data/{location}/daily.csv'
    if os.path.exists(daily_file): 
        with open(daily_file, 'r') as f:
            r = csv.reader(f)
            existing_data = dict(list(r))
    else:
        existing_data = {}
    d, v = daily[location]
    existing_data[d] = v

    with open(daily_file, 'w') as f:
        writer = csv.writer(f)
        writer.writerows((d, existing_data[d]) for d in sorted(existing_data))

# save daily json points
for location, points in data.items():
    year, month, day = date.split('-')
    datadir = f'data/{location}/{year}'
    if not os.path.isdir(datadir):
        os.makedirs(datadir)
    out = {
        'location': location,
        'date': date,
        'data': [{'timestamp': p['timestamp'].isoformat(), 'value': p['value']} for p in points]
    }
    with open(f'{datadir}/{date}.json', 'w') as f:
        json.dump(out, f, indent=2)