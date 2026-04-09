#!/usr/bin/env python3
import sys
import csv

INVALID_TEMP = 9999.9

def main():
    reader = csv.reader(sys.stdin)
    header = None
    station_idx = date_idx = temp_idx = None

    for fields in reader:
        # First row of each file is the header — capture column indices from it
        if header is None or fields[0].strip() == "STATION":
            header = [f.strip() for f in fields]
            try:
                station_idx = header.index("STATION")
                date_idx    = header.index("DATE")
                temp_idx    = header.index("TEMP")
            except ValueError:
                header = None
            continue

        if len(fields) <= max(station_idx, date_idx, temp_idx):
            continue

        station  = fields[station_idx].strip()
        date     = fields[date_idx].strip()
        temp_str = fields[temp_idx].strip()

        # Extract year from DATE (first 4 characters)
        if len(date) < 4:
            continue
        year = date[:4]

        # Handle missing or invalid TEMP values
        try:
            temp = float(temp_str)
        except ValueError:
            continue

        if temp == INVALID_TEMP:
            continue

        # Emit key-value pair: (station, year) -> (temp, 1)
        print(f"{station}\t{year}\t{temp}\t1")

if __name__ == "__main__":
    main()
