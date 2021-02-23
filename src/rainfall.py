import csv
import datetime
import itertools
import json
import os
import optparse
import urllib.request


TESTING = True
DEFAULT_DATE = (datetime.date.today() - datetime.timedelta(days=2)).isoformat()



def get_stations(filename='stations.txt'):
    with open(filename, 'r') as f:
        stations = json.load(f)
    return stations

def fetch_rainfall_from_file(file):
    with open(file, 'rb'):
        return csv.DictReader((l.decode('utf-8') for l in resp.readlines()), )

def fetch_rainfall_data(date):
    url = f"http://environment.data.gov.uk/flood-monitoring/archive/readings-{date}.csv"
    resp = urllib.request.urlopen(url)
    return csv.DictReader((l.decode('utf-8') for l in resp.readlines()), )

def filter_data(raw_data, stations):
    data = {s['location']: [] for s in stations}
    for row in raw_data:
        for station in stations:
            loc = station['location']
            sid = station['station_id']
            if sid in row['measure']:
                timestamp = datetime.datetime.strptime(row['dateTime'], '%Y-%m-%dT%H:%M:%SZ')
                data[loc].append({'timestamp': timestamp, 'value': float(row['value'])})
    return data

def process_hourly(data):
    # group by hour
    hourly = {}
    sortkey = lambda x: x['timestamp'].hour
    for location, points in data.items():
        points = sorted(points, key=sortkey)
        grouped = itertools.groupby(points, sortkey)
        hours = ((key, sum(point['value'] for point in group)) for key, group in grouped)
        hours = [(today + datetime.timedelta(hours=hour), value) for hour, value in hours]
        hourly[location] = hours
    return hourly

def save_hourly_csv(hourly):
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


def save_daily_csv(daily):
    # group by day
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

def save_daily_json(data):
    # save daily json points
    for location, points in data.items():
        datadir = f'data/{location}/{year}'
        if not os.path.isdir(datadir):
            os.makedirs(datadir)
        out = {
            'location': location,
            'date': date,
            'data': [{'timestamp': p['timestamp'].isoformat(), 'value': p['value']} for p in sorted(points, key=lambda x: x['timestamp'])]
        }
        with open(f'{datadir}/{date}.json', 'w') as f:
            json.dump(out, f, indent=2)


if __name__ == '__main__':
    parser = optparse.OptionParser()

    parser.add_option('-s', '--stations', default='./stations.txt')
    parser.add_option('-d', '--date', default=DEFAULT_DATE)
    parser.add_option('-f', '--file')

    options, args = parser.parse_args()

    stations = get_stations(options.stations)

    date = options.date
    year, month, day = date.split('-')
    today = datetime.datetime(int(year), int(month), int(day))

    if options.file:
        dd = fetch_rainfall_from_file(options.file)
    else:
        dd = fetch_rainfall_data(date)

    data = filter_data(dd, stations)

    hourly = process_hourly(data)
    daily = {location: (today.strftime('%Y-%m-%d'), sum(v[1] for v in values)) for location, values in hourly.items()}
    save_daily_csv(daily)
    save_hourly_csv(hourly)
    save_daily_json(data)