import subprocess
import glob
import time

LOG_FILE = "task3.log"

csv_files = glob.glob('./data/**/*.csv', recursive=True)

if len(csv_files) != 0:
    start_time = time.time()

    with open(LOG_FILE, "w") as log:

      
        all_mapped = []
        for i, csv_file in enumerate(csv_files):
            if i % 50 == 0:
            with open(csv_file, 'r', errors='ignore') as f:
                map_proc = subprocess.run(
                    ['python3', 'mapper.py'],
                    stdin=f,
                    capture_output=True,
                    text=True
                )
                if map_proc.stdout.strip():
                    all_mapped.extend(map_proc.stdout.strip().split('\n'))
      
       
        sorted_output = sorted([line for line in all_mapped if line])
        
        reduce_proc = subprocess.run(
            ['python3', 'reducer.py'],
            input='\n'.join(sorted_output),
            capture_output=True,
            text=True
        )
        log.write(reduce_proc.stdout)

        elapsed = time.time() - start_time

        if reduce_proc.returncode == 0:
            results = reduce_proc.stdout.strip().split('\n')
            print(f"\nExecution time: {elapsed:.2f} seconds")
        
