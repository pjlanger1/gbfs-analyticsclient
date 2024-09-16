#V 0.0 of GBFS analytics client
import requests
import json
import time
import logging
import schedule

def systems_wrapper(k):
    systems = {
        'dc': 'https://gbfs.capitalbikeshare.com/gbfs/2.3/gbfs.json',
        'nyc': 'https://gbfs.lyft.com/gbfs/2.3/bkn/gbfs.json',
        'boston':'https://gbfs.bluebikes.com/gbfs/gbfs.json',
        'chicago': 'https://gbfs.divvybikes.com/gbfs/2.3/gbfs.json',
        'sf': 'https://gbfs.baywheels.com/gbfs/2.3/gbfs.json',
        'portland': 'https://gbfs.biketownpdx.com/gbfs/2.3/gbfs.json',
        'denver': 'https://gbfs.lyft.com/gbfs/2.3/den/gbfs.json',
        'columbus': 'https://gbfs.lyft.com/gbfs/2.3/cmh/gbfs.json', 
        'la':'https://gbfs.bcycle.com/bcycle_lametro/gbfs.json',
        'phila':'https://gbfs.bcycle.com/bcycle_indego/gbfs.json',
        'toronto':'https://tor.publicbikesystem.net/customer/gbfs/v2/gbfs.json'
    }
    if k in systems:
        return gbfs_feed(k, systems[k])

class gbfs_feed:
    def __init__(self, sysinit, baseurl):
        self.logger = logging.getLogger(__name__)
        self.system = sysinit
        self.baseurl = baseurl
        self.cache = {}
        self.ttls = {}  #use this to carry feed-specific ttls, nothing in dict if not used
        self.scheduled_tasks = {}
        self.timeseries_store = {}  #dictionary to store instances of gbfs_timeseries
        self.stop_time = None  #to track when the scheduling should stop

        try:
            self.ttl, self.vers, self.urls = self.get_feed_info()
        except Exception as e:
            self.logger.error(f"Failed to initialize GBFS feed for {sysinit}: {e}")
            self.ttl, self.vers, self.urls = None, None, {}

    def safe_request_handler(self, url, expect_json=True, use_cache=True):
        headers = {}
        if use_cache and url in self.cache:
            etag = self.cache[url].get('etag')
            if etag:
                headers['If-None-Match'] = etag

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            #handle 304 not modified
            if response.status_code == 304 and use_cache:
                self.logger.info(f"Using cached data for {url}")
                return self.cache[url]['data']  #return cached data if 304 Not Modified

            #if not 304 or caching is not used, process the response normally
            if expect_json:
                data = response.json()
            else:
                data = response.content

            #updates the cache if caching is enabled
            if use_cache:
                self.cache[url] = {
                    'etag': response.headers.get('ETag'),
                    'data': data
                }
            return data
        except requests.RequestException as e:
            self.logger.error(f"HTTP request failed: {e}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding JSON: {e}")
        return None

    def get_feed_info(self):
        resp = self.safe_request_handler(self.baseurl, use_cache=False)
        if resp is None:
            return None, 'error', {}

        ttl = resp.get('ttl', '')
        vers = resp.get('version', 'error')
        last_updated = resp.get('last_updated', 0)
        if abs(time.time() - last_updated) > 1000:
            self.logger.warning(f'Warning: Data last updated at {last_updated}')

        dic = {i['name']: i['url'] for i in resp.get('data', {}).get('en', {}).get('feeds', [])}  #selects language automatically as 'en'

        return ttl, vers, dic

    def info(self):
        if self.urls:
            print('Available feeds: ' + ', '.join(self.urls.keys()))
        else:
            print("No feeds available.")

    def explore(self, feed):
        if feed in self.urls:
            data = self.safe_request_handler(self.urls[feed], use_cache=True)
            if data:
                print(data.get('last_updated', 0))
                print(data.get('data', {}))
            else:
                print(f'Feed {feed} not found or failed to fetch.')
        else:
            print(f'Feed {feed} not found')

    def compose(self, experiment_type, feed, interval=3600, duration=86400):
        iterations = (duration // interval)-1  #calculate number of iterations
        if experiment_type == 'snapshot':
            self.perform_snapshot(feed, interval, iterations)
        elif experiment_type == 'delta':
            self.perform_delta(feed, interval, iterations)
        else:
            self.logger.error("Invalid experiment type specified.")
        
        #set the stop time based on the duration
        self.stop_time = time.time() + duration
        #start the scheduler loop
        self.set_schedule()

    def perform_snapshot(self, feed, interval, iterations):
        """Create a new timeseries instance and perform periodic snapshots."""
        timeseries = gbfs_timeseries(feed)  #create new timeseries instance
        self.timeseries_store[feed] = timeseries  #store instance in dictionary
        task_counter = [0]  #using a list to make it mutable inside the inner function

        def task():
            if task_counter[0] >= iterations:
                return schedule.CancelJob  #ends the job after the specified number of iterations
            data = self.safe_request_handler(self.urls[feed])
            if data:
                timeseries.init_snapshot(data)  #store snapshot in the timeseries object
                with open(f'{feed}_snapshot.json', 'w') as f:
                    json.dump(data, f, indent=4)  #indent for pretty printing
                print(f"Snapshot for {feed} taken at {time.ctime()}")  #example logging
            task_counter[0] += 1

        #schedule the periodic task
        schedule.every(interval).seconds.do(task)

        #trigger the first task immediately
        task()

    def perform_delta(self, feed, interval, iterations):
        """Create a new timeseries instance and calculate deltas between periodic pulls."""
        old_data = None
        timeseries = gbfs_timeseries(feed)  #create new timeseries instance
        self.timeseries_store[feed] = timeseries  #store instance in dictionary
        task_counter = [0]  #using a list to make it mutable inside the inner function

        def task():
            nonlocal old_data  #needed to retain the state between calls
            if task_counter[0] >= iterations:
                return schedule.CancelJob  #ends the job after the specified number of iterations
            current_data = self.safe_request_handler(self.urls[feed])
            if current_data:
                if old_data is not None:
                    delta = self.calculate_delta(old_data, current_data)
                    timeseries.init_delta(old_data, current_data)  #store delta in the timeseries object
                    with open(f'{feed}_delta.json', 'w') as f:
                        json.dump(delta, f, indent=4)
                    print(f"Delta for {feed} analyzed at {time.ctime()}")  #example logging
                old_data = current_data
            task_counter[0] += 1

        #schedule the periodic task
        schedule.every(interval).seconds.do(task)

        #trigger the first task immediately
        task()

    def calculate_delta(self, old_data, new_data):
        """Calculate the delta between old and new data."""
        delta = {key: new_data[key] for key in new_data if key not in old_data or new_data[key] != old_data[key]}
        return delta

    def get_timeseries(self, feed):
        """Retrieve the stored timeseries for a specific feed."""
        return self.timeseries_store.get(feed, None)

    def set_schedule(self):
        """Run the schedule loop and stop after the duration."""
        print("Starting scheduler...")
        while time.time() < self.stop_time:
            schedule.run_pending()
            time.sleep(1)  
            #blocking wait to prevent high CPU usage. 
            #this is controversial, but we want this to be blocking so that a user behavior on the same python kernel 
            #won't impact cadence of data pulls
        
        #clear all scheduled jobs after the duration ends
        schedule.clear()
        print("Scheduling ended after the specified duration.")


class gbfs_timeseries:
    def __init__(self, feed_name):
        self.feed_name = feed_name
        self.data = {}  #to store time series data

    def init_snapshot(self, feed_data, station_info_data=None):
        """Initialize snapshot for a specific feed."""
        if self.feed_name == 'station_information':
            #populate station manifest with static information
            self.restructure_station_information(feed_data)

        elif self.feed_name == 'station_status':
            #ensure station manifest is populated from station_information
            if station_info_data is not None:
                self.restructure_station_information(station_info_data)

            #begin restructuring for station status
            self.restructure_station_status(feed_data)

        elif self.feed_name == 'free_bike_status':
            #maintain a bike inventory
            self.restructure_free_bike_status(feed_data)

    def init_delta(self, old_data, new_data):
        """Calculate delta between old and new data for time series analysis."""
        if self.feed_name == 'station_status' or self.feed_name == 'free_bike_status':
            delta_data = self.calculate_delta(old_data, new_data)
            self.update_data_with_delta(delta_data)
        else:
            print(f"Delta experiment not supported for feed type: {self.feed_name}")

    def restructure_station_information(self, station_info):
        """Restructure station information data."""
        for station in station_info.get('data', {}).get('stations', []):
            station_id = station['station_id']
            self.data[station_id] = {time.time(): station}
            #development area for in process module 2

    def restructure_station_status(self, station_status):
        """Restructure station status data and handle updates."""
        for station in station_status.get('data', {}).get('stations', []):
            station_id = station['station_id']
            #if station_id not in self.data:
            #    print(f"New station {station_id} detected. Refreshing station information...")
            #    #here, you'd recall station_information to update your manifest.
            self.data.setdefault(station_id, {})[time.time()] = station

    def restructure_free_bike_status(self, free_bike_status):
        """Maintain bike inventory with new updates."""
        uptime = free_bike_status.get('last_updated',0)
        for bike in free_bike_status.get('data', {}).get('bikes', []):
            bike_id = bike['bike_id']
            self.data.setdefault(bike_id, {})[uptime] = bike
    

    def calculate_delta(self, old_data, new_data):
        """Calculate delta between two data points."""
        delta = {key: new_data[key] for key in new_data if key not in old_data or new_data[key] != old_data[key]}
        return delta

    def update_data_with_delta(self, delta_data):
        """Update time series data with calculated deltas."""
        for key, value in delta_data.items():
            self.data.setdefault(key, {})[time.time()] = value

    def display(self):
        """Utility function to display the current state of time series data."""
        print(json.dumps(self.data, indent=4))


