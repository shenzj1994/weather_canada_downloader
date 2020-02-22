import datetime
import logging
import os
import re
import time
import urllib.parse as urlparse

import requests

from proxies import Proxies

logging.basicConfig(level=logging.INFO)


class CanWeatherDataDownloader:
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H%M%S')
    download_dir = "./download/" + st + "/"
    output_format = 'csv'
    station_id_list = []

    def __init__(self):
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)

    @staticmethod
    def build_data_download_url(output_format, station_id, year, month, day, time_frame):
        """
        Build the downloadable URL.

        @param output_format: Output format
        @param station_id: Station ID
        @param year: Year
        @param month: Month
        @param day: Day
        @param time_frame: Time Frame. 1 for hourly, 2 for daily, 3 for monthly.
        @return: A downloadable url
        """
        endpoint_url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?"
        params = {'format': output_format, 'stationID': station_id, 'Year': year, 'Month': month,
                  'Day': day,
                  'timeframe': time_frame}

        url_parts = list(urlparse.urlparse(endpoint_url))
        query = dict(urlparse.parse_qsl(url_parts[4]))
        query.update(params)

        url_parts[4] = urlparse.urlencode(query)

        url_to_get = urlparse.urlunparse(url_parts)
        # print(url_to_get)
        return url_to_get

    @staticmethod
    def read_station_id_from_file(file):
        """
        Read all station IDs from file

        @param file: File contains station IDs
        @return: A list contains all the station IDs
        """
        with open(file) as id_f:
            station_id_list = id_f.readlines()
            logging.info("Total number of stations: " + str(len(station_id_list)))
        return station_id_list

    @staticmethod
    def download_from_url(url, destination):
        """
        Download single file from URL using the file name from 'content-disposition'.
        @type url: str
        @type destination: str

        @param url: URL to download
        @param destination: Destination folder to store file
        """
        logging.info("downloading: ", url)

        r = requests.get(url, proxies=Proxies.get_random_proxy(), stream=True)
        d = r.headers['content-disposition']
        fname: str = re.findall("filename=(.+)", d)[0]

        if r.status_code == requests.codes.ok:
            with open(destination + fname.replace('"', ''), 'wb+') as download_f:
                for data in r:
                    download_f.write(data)
        return url

    def download_daily_data(self, station_id_list, start_year, end_year):
        """
        Download daily data between start year and end year (inclusive)
        @type station_id_list: list
        @type start_year: int
        @type end_year: int

        @param station_id_list: A list contains station ID. One for each list element
        @param start_year: start year (inclusive)
        @param end_year: end year (inclusive)
        """
        download_urls = []
        for y in range(start_year, end_year + 1):
            for s_id in station_id_list:
                s_url = self.build_data_download_url(self.output_format, s_id[0], y, 1, 1, 2)
                download_urls.append(s_url)
        logging.info("Total number of URLs: " + str(len(download_urls)))
        for u in download_urls:
            self.download_from_url(u, self.download_dir)

# TODO: Add Station ID to each data file
