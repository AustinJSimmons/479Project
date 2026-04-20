%%writefile mapper.py
import sys
import csv

reader = csv.reader(sys.stdin)
header = next(reader)
header = [h.strip() for h in header]

station_idx = header.index('STATION')
date_idx    = header.index('DATE')
temp_idx    = header.index('TEMP')

for parts in reader:
    if len(parts) <= max(station_idx, date_idx, temp_idx):
        continue
    station = parts[station_idx].strip()
    date    = parts[date_idx].strip()
    temp    = parts[temp_idx].strip()
    if len(date) < 4 or temp == '9999.9':
        continue
    year = date[:4]
    print(f"{station}_{year}\t{temp}")
