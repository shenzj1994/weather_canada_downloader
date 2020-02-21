import csv
import datetime
import logging
import os
import re
import time
import urllib.parse as urlparse
from multiprocessing.pool import ThreadPool
from urllib.parse import urlencode

import requests

st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H%M%S')

logging.basicConfig(level=logging.INFO)

# User-defined proxies
proxies = {
    "http": "",
    "https": "",
}


# Find URL for a single station id in a year
def build_data_download_url(output_format, station_id, year, month=1, day=1, time_frame=2):
    endpoint_url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?"
    params = {'format': output_format, 'stationID': station_id, 'Year': year, 'Month': month,
              'Day': day,
              'timeframe': time_frame}

    url_parts = list(urlparse.urlparse(endpoint_url))
    query = dict(urlparse.parse_qsl(url_parts[4]))
    query.update(params)

    url_parts[4] = urlencode(query)

    url_to_get = urlparse.urlunparse(url_parts)
    # print(url_to_get)
    return url_to_get


# Read all station IDs from CSV
with open('station_id.csv', newline='') as id_f:
    reader = csv.reader(id_f)
    bc_station_id_list = tuple(reader)
    logging.info("Total number of stations: " + str(len(bc_station_id_list)))

download_urls = []
for y in range(1990, 2021):
    for s_id in bc_station_id_list:
        s_url = build_data_download_url('csv', s_id[0], y)
        download_urls.append(s_url)
logging.info("Total number of URLs: " + str(len(download_urls)))

download_dir = "./download/" + st + "/"
if not os.path.exists(download_dir):
    os.makedirs(download_dir)


# Download file from URL using the file name from content-disposition
def download_url(url):
    print("downloading: ", url)

    r = requests.get(url, proxies=proxies, stream=True)
    d = r.headers['content-disposition']
    fname: str = re.findall("filename=(.+)", d)[0]

    if r.status_code == requests.codes.ok:
        with open(download_dir + fname.replace('"', ''), 'wb+') as download_f:
            for data in r:
                download_f.write(data)
    return url


# Run multiple threads. Each call will take the next element in urls list
results = ThreadPool(50).imap_unordered(download_url, download_urls)
for r in results:
    print(r)

# TODO: Add Station ID to each data file
