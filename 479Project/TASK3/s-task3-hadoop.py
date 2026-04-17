import subprocess
import os
import time
from dotenv import load_dotenv

load_dotenv()

# Config
STORAGE_ACCOUNT_NAME = "groupdata479storage"
STORAGE_ACCOUNT_KEY  = os.environ["AZURE_STORAGE_KEY"]
CONTAINER_NAME       = "gsod-data"

AZURE_INPUT  = f"wasbs://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/*/01*.csv"
HDFS_OUTPUT  = "/user/hadoop/s-task3-output"

SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
MAPPER        = os.path.join(SCRIPT_DIR, "s-mapper.py")
REDUCER       = os.path.join(SCRIPT_DIR, "s-reducer.py")

STREAMING_JAR = "/opt/homebrew/Cellar/hadoop/3.4.3/libexec/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar"
AZURE_JAR     = "/opt/homebrew/Cellar/hadoop/3.4.3/libexec/share/hadoop/common/lib/hadoop-azure-3.4.3.jar"
STORAGE_JAR   = "/opt/homebrew/Cellar/hadoop/3.4.3/libexec/share/hadoop/common/lib/azure-storage-7.0.1.jar"

FS_KEY = f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "s-task3-hadoop.log")

def run_job():
    subprocess.run(["hadoop", "fs", "-rm", "-r", "-f", HDFS_OUTPUT],
                   stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

    cmd = [
        "hadoop", "jar", STREAMING_JAR,
        "-libjars", f"{AZURE_JAR},{STORAGE_JAR}",
        "-D", f"{FS_KEY}={STORAGE_ACCOUNT_KEY}",
        "-input",   AZURE_INPUT,
        "-output",  HDFS_OUTPUT,
        "-mapper",  "python3 s-mapper.py",
        "-reducer", "python3 s-reducer.py",
        "-file",    MAPPER,
        "-file",    REDUCER,
    ]

    print("Submitting Hadoop Streaming job...")
    print(f"Hadoop logs being written to: {LOG_FILE}")

    start_time = time.time()

    with open(LOG_FILE, "w") as log:
        result = subprocess.run(cmd, stderr=log, stdout=log)

    elapsed = time.time() - start_time

    if result.returncode == 0:
        print("\nJob completed successfully.")
        print(f"Output written to HDFS at: {HDFS_OUTPUT}")
        print("\n--- Results ---")
        subprocess.run(["hadoop", "fs", "-cat", f"{HDFS_OUTPUT}/part-*"],
                       stderr=subprocess.DEVNULL)
        print(f"\nTotal execution time: {elapsed:.2f} seconds")
    else:
        print(f"\nJob failed after {elapsed:.2f} seconds. Check {LOG_FILE} for details.")

if __name__ == "__main__":
    run_job()