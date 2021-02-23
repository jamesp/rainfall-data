import datetime

import rainfall

basedate = datetime.date(2021,1,1)
num_days = 70

for n in range(num_days):
    date = (basedate + datetime.timedelta(days=n)).isoformat()
    print(f'running {date}')
    stations = rainfall.get_stations('stations.txt')
    raw_data = rainfall.fetch_rainfall_data(date)
    data = rainfall.filter_data(raw_data, stations)
    hourly = rainfall.process_hourly(data)
    daily = {location: (date, sum(v[1] for v in values)) for location, values in hourly.items()}
    rainfall.save_daily_csv(daily)
    rainfall.save_hourly_csv(hourly)
    rainfall.save_daily_json(data)