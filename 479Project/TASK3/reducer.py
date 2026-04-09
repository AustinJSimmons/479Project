#!/usr/bin/env python3
import sys

def main():
    current_key = None
    temp_sum = 0.0
    temp_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) != 4:
            continue

        station, year, temp_str, count_str = parts

        try:
            temp = float(temp_str)
            count = int(count_str)
        except ValueError:
            continue

        key = (station, year)

        if current_key == key:
            # Same (station, year): accumulate
            temp_sum += temp
            temp_count += count
        else:
            # New key: emit the previous group's result
            if current_key is not None:
                avg_temp = temp_sum / temp_count
                prev_station, prev_year = current_key
                print(f"{prev_station}, {prev_year}, {avg_temp:.2f}")

            # Reset for new key
            current_key = key
            temp_sum = temp
            temp_count = count

    # Emit the final group
    if current_key is not None and temp_count > 0:
        avg_temp = temp_sum / temp_count
        prev_station, prev_year = current_key
        print(f"{prev_station}, {prev_year}, {avg_temp:.2f}")

if __name__ == "__main__":
    main()
