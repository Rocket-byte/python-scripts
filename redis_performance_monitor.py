import redis
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import threading
import csv
from datetime import datetime
import prettytable
import os
import platform
import logging

# Author: Ruslana Kruk 2022
# This script measures and monitors the performance of Redis server connections.

# Setup logging
logging.basicConfig(filename='db_errors.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Define constants
THREADS = 10
ITERATIONS_PER_THREAD = 50

# Redis connection parameters
redis1_params = {"host": "redis1-master", "port": 6379, "db": 0}
redis2_params = {"host": "redis2-master", "port": 6379, "db": 0}

def clear_screen():
    os.system('cls' if platform.system().lower() == "windows" else 'clear')

def connect_to_redis(redis_params):
    try:
        start_time = time.time()
        redis.Redis(**redis_params)
        return time.time() - start_time, threading.current_thread().ident, True
    except Exception as e:
        logging.error(f'Redis Connection error: {e}')
        return time.time() - start_time, threading.current_thread().ident, False

def write_read_redis_keys(redis_params):
    r = redis.Redis(**redis_params)
    keys, values = [f"test_key_{i}" for i in range(100)], [f"test_value_{i}" for i in range(100)]
    write_start_time = time.time()
    for key, value in zip(keys, values):
        r.set(key, value)
    write_time = time.time() - write_start_time

    read_start_time = time.time()
    for _ in range(10):
        for key in keys:
            r.get(key)
    read_time = (time.time() - read_start_time) / 1000
    return write_time, read_time

def measure_connection_time(redis_params):
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        results = list(tqdm(executor.map(lambda _: connect_to_redis(redis_params), range(ITERATIONS_PER_THREAD * THREADS)),
                            total=ITERATIONS_PER_THREAD * THREADS, desc=f"Measuring Redis connection times for {redis_params['host']}"))

    times, successes, failures = [result[0] for result in results], sum(result[2] for result in results), THREADS * ITERATIONS_PER_THREAD - sum(result[2] for result in results)
    average_time = sum(times) / len(times)
    failure_percentage = (failures / (successes + failures)) * 100

    write_time, read_time = write_read_redis_keys(redis_params)

    return {
        "Datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Redis Host": redis_params['host'],
        "Total Time": f"{sum(times):.4f}",
        "Avg Connection Time": f"{average_time:.4f}",
        "Success": successes,
        "Failures": failures,
        "Failure %": f"{failure_percentage:.2f}%",
        "Write Time": f"{write_time:.6f}",
        "Average Read Time": f"{read_time:.6f}"
    }

def measure_redis_servers(*redis_params):
    return [measure_connection_time(params) for params in redis_params]

def write_to_csv(data, filename='db_connect.csv'):
    headers = data[0].keys()
    with open(filename, 'a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        for row in data:
            writer.writerow(row)

def print_table(data):
    table = prettytable.PrettyTable()
    table.field_names = data[0].keys()
    for row in data:
        table.add_row([row[key] for key in table.field_names])
    print(table)

last_results = []

while True:
    results = measure_redis_servers(redis1_params, redis2_params)
    last_results.append(results)
    if len(last_results) > 60:
        last_results.pop(0)
    clear_screen()
    print_table(last_results)
    write_to_csv(results)
    time.sleep(60)
