import requests
import time
import concurrent.futures
from pprint import pprint as pp

titles = [
    'Memento',
    'The Godfather',
    'Kac Wawa',
    'The Shawshank Redemption',
    'In Bruges',
    'Gods'
]

results = []


def download_data(title):
    payload = {'t': title,
               'r': 'json'}  # Get full data based on title in json format
    respond = requests.get(f'http://www.omdbapi.com/?apikey=305043ae',
                           params=payload).json()
    results.append(respond)
    print(f'Done downloading {title}')
    pp(len(results))
    print()
    return results


t1 = time.perf_counter()

with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(download_data, titles)

t2 = time.perf_counter()

print(f'Finished in {t2 - t1} s')
