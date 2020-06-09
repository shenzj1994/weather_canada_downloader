import datetime
import time

from combiner import CSVCombiner
from downloader import CanWeatherDataDownloader
from proxies import Proxies

st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H%M%S')

dldr = CanWeatherDataDownloader()

dldr.set_download_dir = './download/' + st + '/'
dldr.set_proxies(Proxies.get_random_proxy())
dldr.all_station_id = dldr.read_station_id_from_file('./station_id.txt')
dldr.download_daily_data(1990, 2020, thread=20)

csv_combiner = CSVCombiner(dldr.download_dir, 'weather_data.csv').run()
