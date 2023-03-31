"""
Convert all daily data into a single csv file
"""

import sys
from pathlib import Path
import csv

base_path: Path = Path(__file__).parent.parent

data_dir = base_path / "data"
outfile = sys.argv[1]

with open("output.csv", "w") as fh:
    writer = csv.writer(fh)
    header = ['time', 'rainfall_mm', 'site']
    writer.writerow(header)
    for site in data_dir.iterdir():
        loc = site.name
        for file in site.iterdir():
            if file.suffix == '.csv' and file.name.startswith('20'):
                with open(site/"daily.csv") as inp:
                    reader = csv.reader(inp)
                    header = reader.__next__()
                    writer.writerows((*r, loc) for r in reader)
        

    

