import sys

current_key   = None
current_total = 0
current_count = 0

for line in sys.stdin:
    line = line.strip()
    try:
        key, value = line.split('\t', 1)
    except ValueError:
        continue
    if current_key == key:
        current_total += float(value)
        current_count += 1
    else:
        if current_key:
            print(f"{current_key}\t{current_total/current_count:.2f}")
        current_key   = key
        current_total = float(value)
        current_count = 1

if current_key:
    print(f"{current_key}\t{current_total/current_count:.2f}")
