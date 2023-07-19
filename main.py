'''
Note for Mirjam:
Do: add parameters into functions to remove large block at top
test run
add parser#
'''


import datetime
import sqlite3
import sys
from math import radians, sin, cos, sqrt, asin
import numpy as np
import rasterio
import logging
from collections import defaultdict, Counter
import csv
from scipy.interpolate import CubicSpline
from sklearn.cluster import DBSCAN

# This is a simple python script to fill the database with data from the Global Fishing Watch csv-files.
# Author: Sören Dethlefsen
# Supervision: Mirjam Bayer


# create parser of parameters

# fishing_types = ['Data/toclean/drifting_longlines.csv','Data/toclean/fixed_gear.csv','Data/toclean/pole_and_line.csv','Data/toclean/purse_seines.csv','Data/toclean/trawlers.csv','Data/toclean/trollers.csv','Data/toclean/unknown.csv']
fishing_types = ['Data/toclean/fixed_gear.csv']
dma = []


class Pipeline:
    def __init__(self, dma_files, 
                 iFile:str='fixed_gear.csv', 
                 dataDir:str='./data/',
                 datasource:str='GFw', # make possible to either input GFW or DMA something along the lines of  default=((1, 2), (3, 4)), nargs=2
                 database:str='vessel_data.db',
                 table_name:str='vessel_data',
                 databaseDir:str='./data/',
                 performanceMode:bool=True,
                 shore_threshold:float=10.0,
                 anchorage_threshold:float=0.5,
                 speed_lat_lon_cleaning:bool=True,
                 speed_threshold:int=30,
                 distance_lat_lon_cleaning:bool=False,
                 distance_threshold:int=100,
                 time_lat_lon_cleaning:bool=False,
                 time_threshold:int=1000,
                 max_std:float=2.5, 
                 interpolation_lower_threshold:int=600,
                 interpolation_upper_threshold:int=43200,
                 ) -> None:
        


        logging.basicConfig(filename='anomalies.log', level=logging.DEBUG)
        self.csvFile = iFile
        self.dma_files = dma_files

        # Constants for configuration
        self.performanceMode = performanceMode  # Performance mode will deactivate all DBSCAN clusterings making the data less correct. Performance mode is strongly recommended for computers with 16 gb RAM or less!

        self.shore_threshold = shore_threshold  # Maximum distance to the nearest shore. Used to find the anchorage points of a vessel. In meters.
        self.anchorage_threshold = anchorage_threshold  # Used to detect trips. If in this distance to the nearest anchorage it accounts as a stop for a trip. In kilometers

        self.speed_lat_lon_cleaning = speed_lat_lon_cleaning # Choose if the coordinates should be cleaned with the help of the calculatedSpeed
        self.speed_threshold = speed_threshold  # Threshold for the calculated Speed in cleaning of lat_lon when choosing the calculatedSpeed Option
        self.distance_lat_lon_cleaning = distance_lat_lon_cleaning
        self.distance_threshold = distance_threshold  # Maximum distance used to check if a coordinate position is plausible and as maximum distance for DBSCAn algorithms. In kilometers
        self.time_lat_lon_cleaning = time_lat_lon_cleaning
        self.time_threshold = time_threshold # Threshold for timeDelta when choosing the Option timeDelta while cleaning lat_lon

        self.max_std = max_std  # The maximun standard deviation used to plausible the speed. In x times the standard deviation
        self.interpolation_lower_threshold = interpolation_lower_threshold # Threshold for timedistance to begin interpolation. Example 600 = 10min: If the timedistance if over 10min the records between two points are interpolate so that the timedistance should be near one minute for every value
        self.interpolation_upper_threshold = interpolation_upper_threshold # Threshold for timedistance to not interpolate, because the data would be nonsense. So interpolation happens if the timedistance is over the lower threshold and under the upper threshold.

        # Indexes from the shipData for columns
        self.d_mmsi = 0
        self.d_timestamp = 1
        self.d_distance_from_shore = 2
        self.d_distance_from_port = 3
        self.d_speed = 4
        self.d_calculatedSpeed = 5
        self.d_course = 6
        self.d_lat = 7
        self.d_lon = 8
        self.d_isFishing = 9
        self.d_source = 10
        self.d_dataset = 11
        self.d_readableTimestamp = 12
        self.d_distanceDelta = 13
        self.d_timeDelta = 14
        self.d_anchorageDelta = 15
        self.d_waterDepth = 16
        self.d_tripCount = 17

        # Indexes for the GFW - csv columns
        self.c_mmsi = 0
        self.c_timestamp = 1
        self.c_distance_from_shore = 2
        self.c_distance_from_port = 3
        self.c_speed = 4
        self.c_course = 5
        self.c_lat = 6
        self.c_lon = 7
        self.c_is_fishing = 8
        self.c_source = 9

        # Indexes for DMA - csv columns
        self.m_mmsi = 2
        self.m_timestamp = 0
        self.m_speed = 6
        self.m_course = 7
        self.m_lat = 3
        self.m_lon = 4
        self.m_is_fishing = 5

        
        self.connect_to_database(database)
        self.create_table(table_name=table_name)

        if datasource == 'GFW': self.fill_database_with_GFW()
        
        elif datasource == 'DMA': self.fill_database_with_DMA()
        
        else: print("Datasource not in {GFW, DMA}")
        
        # self.interpolation_rec()
        # print("[XXXX         ] Interpolation done!")
        self.calculate_deltas()
        self.calculate_speed()
        #self.adjust_speed()
        self.delete_lat_lon()
        #self.add_bathymetry()
        #self.anchorage_detection()
        #self.trip_detection()
        #self.export_database()
        self.close_connection(database)

        print("[XXXXXXXXXXXXX] SUCCEEEEESS!")
    
    def connect_to_database(self, database):
        self.conn = sqlite3.connect(database)
        self.c = self.conn.cursor()
    
    def close_connection(self, datanse):
        self.conn.close()
        # test if runs if no connection is established beforehand
    
    
    def create_table(self, table_name, datasource):
        if datasource=='GFW':
            self.c.execute(f"""
                            CREATE TABLE IF NOT EXISTS {table_name} (
                                mmsi REAL,
                                timestamp REAL,
                                distance_from_shore REAL,
                                distance_from_port REAL,
                                speed REAL,
                                calculatedSpeed REAL,
                                course REAL,
                                lat REAL,
                                lon REAL,
                                is_fishing REAL,
                                source TEXT,
                                dataset TEXT,
                                timestampR TEXT,
                                distanceDelta REAL,
                                timeDelta REAL,
                                anchorageDelta REAL,
                                waterDepth REAL,
                                tripCount Int,
                                PRIMARY KEY (mmsi, timestamp)
                            )
                        """)
        print("[X           ] Created table!")

    def fill_database_with_GFW(self):
        duplicate_counter = 0
        # iterate through all files
        for file_path in self.csvFile:
            # Open the CSV file
            with open(file_path, 'r') as file:
                next(file)  # Read the file line by line (& skip first line)
                for line in file:
                    # Split the line into fieldsgit
                    fields = line.strip().split(',')

                    # Process the fields
                    try:
                        mmsi = int(float(fields[self.c_mmsi]))
                    except Exception as e:
                        mmsi = None
                        logging.error("mmsi: " + fields[self.c_mmsi] + " " + str(e) + "\n")

                    timestamp = fields[self.c_timestamp]

                    try:
                        distance_from_shore = int(float(fields[self.c_distance_from_shore]))
                    except Exception as e:
                        distance_from_shore = None
                        logging.warning(
                            "distance_from_shore: " + fields[self.c_distance_from_shore] + " " + str(e) + "\n")

                    try:
                        distance_from_port = int(float(fields[self.c_distance_from_port]))
                    except Exception as e:
                        distance_from_port = None
                        logging.warning(
                            "distance_from_port: " + fields[self.c_distance_from_port] + " " + str(e) + "\n")

                    try:
                        speed = round(float(fields[self.c_speed]), 2)
                    except Exception as e:
                        speed = None
                        logging.warning("speed: " + fields[self.c_speed] + " " + str(e) + "\n")

                    try:
                        course = round(float(fields[self.c_course]), 2)
                    except Exception as e:
                        course = None
                        logging.warning("course: " + fields[self.c_course] + " " + str(e) + "\n")

                    try:
                        lat = round(float(fields[self.c_lat]), 5)
                    except Exception as e:
                        lat = None
                        logging.warning("lat: " + fields[self.c_lat] + " " + str(e) + "\n")

                    try:
                        lon = round(float(fields[self.c_lon]), 5)
                    except Exception as e:
                        lon = None
                        logging.warning("lon:" + fields[self.c_lon] + " " + str(e) + "\n")

                    is_fishing = fields[self.c_is_fishing]

                    source = fields[self.c_source]

                    dataset = file_path[13:-4]

                    try:
                        timestampR = datetime.datetime.fromtimestamp(int(float(fields[self.c_timestamp]))).strftime(
                            '%Y-%m-%d %H:%M:%S')
                    except Exception as e:
                        timestampR = None
                        logging.warning("TimestampR: " + fields[self.c_timestamp] + " " + str(e) + "\n")

                    try:
                        # fill entries
                        self.c.execute(
                            f"INSERT INTO vessel_data (mmsi, timestamp, distance_from_shore, distance_from_port, speed, course, lat, lon,is_fishing ,source, dataset, timestampR) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (mmsi, timestamp, distance_from_shore, distance_from_port, speed, course,
                             lat,
                             lon, is_fishing, source, dataset, timestampR,))
                    except Exception as e:
                        duplicate_counter += 1
                        logging.warning(
                            "GFW: Duplicate found: mmsi = " + str(mmsi) + " | timestamp = " + str(timestamp))
            self.conn.commit()
            print("GFW: There are: " + str(duplicate_counter) + " duplicates.")
            print("[XX          ] Database filled with GFW Data!")
        

    def fill_database_with_DMA(self):
        duplicate_counter = 0
        # iterate through all files
        for file_path in self.dma_files:

            # Open the CSV file
            with open(file_path, 'r') as file:
                next(file)  # Read the file line by line (& skip first line)
                for line in file:
                    # Split the line into fields
                    fields = line.strip().split(',')

                    # Process the fields
                    try:
                        mmsi = int(float(fields[self.c_mmsi]))
                    except Exception as e:
                        mmsi = None
                        logging.error("mmsi: " + fields[self.c_mmsi] + " " + str(e) + "\n")

                    timestamp = fields[self.c_timestamp]

                    try:
                        distance_from_shore = int(float(fields[self.c_distance_from_shore]))
                    except Exception as e:
                        distance_from_shore = None
                        logging.warning(
                            "distance_from_shore: " + fields[self.c_distance_from_shore] + " " + str(e) + "\n")

                    try:
                        distance_from_port = int(float(fields[self.c_distance_from_port]))
                    except Exception as e:
                        distance_from_port = None
                        logging.warning(
                            "distance_from_port: " + fields[self.c_distance_from_port] + " " + str(e) + "\n")

                    try:
                        speed = round(float(fields[self.c_speed]), 2)
                    except Exception as e:
                        speed = None
                        logging.warning("speed: " + fields[self.c_speed] + " " + str(e) + "\n")

                    try:
                        course = round(float(fields[self.c_course]), 2)
                    except Exception as e:
                        course = None
                        logging.warning("course: " + fields[self.c_course] + " " + str(e) + "\n")

                    try:
                        lat = round(float(fields[self.c_lat]), 5)
                    except Exception as e:
                        lat = None
                        logging.warning("lat: " + fields[self.c_lat] + " " + str(e) + "\n")

                    try:
                        lon = round(float(fields[self.c_lon]), 5)
                    except Exception as e:
                        lon = None
                        logging.warning("lon:" + fields[self.c_lon] + " " + str(e) + "\n")

                    is_fishing = fields[self.c_is_fishing]

                    source = fields[self.c_source]

                    dataset = file_path[13:-4]

                    try:
                        timestampR = datetime.datetime.fromtimestamp(int(float(fields[self.c_timestamp]))).strftime(
                            '%Y-%m-%d %H:%M:%S')
                    except Exception as e:
                        timestampR = None
                        logging.warning("TimestampR: " + fields[self.c_timestamp] + " " + str(e) + "\n")

                    try:
                        # fill entries
                        self.c.execute(
                            f"INSERT INTO vessel_data (mmsi, timestamp, distance_from_shore, distance_from_port, speed, calculatedSpeed, course, lat, lon,is_fishing ,source, dataset, timestampR, distanceDelta, timeDelta) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (mmsi, timestamp, distance_from_shore, distance_from_port, speed, course,
                             lat,
                             lon, is_fishing, source, dataset, timestampR,))
                    except Exception as e:
                        duplicate_counter += 1
                        logging.warning(
                            "DMA: Duplicate found: mmsi = " + str(mmsi) + " | timestamp = " + str(timestamp))
            self.conn.commit()
            logging.warning("DMA: There are: " + str(duplicate_counter) + " duplicates.")
        print("[XXX          ] Database filled with DMA Data!")
        
    
    def calculate_deltas(self):
        # Get every distinct ship id from the database
        self.c.execute('SELECT distinct mmsi FROM vessel_data')
        allShips = self.c.fetchall()

        # for every ship in the database (sorted after time)
        for ship in allShips:
            self.c.execute('SELECT * FROM vessel_data WHERE mmsi = ? ORDER BY mmsi ASC, timestamp ASC', (ship[0],))
            shipData = self.c.fetchall()
            # For every data Point for that mmsi
            for x in range(len(shipData)):
                # only do that if possible (is not the first data entry for mmsi)
                if (x - 1) >= 0:
                    # get parameters
                    mmsi = shipData[x][self.d_mmsi]
                    lat1 = float(shipData[x][self.d_lat])
                    lat2 = float(shipData[x - 1][self.d_lat])
                    lon1 = float(shipData[x][self.d_lon])
                    lon2 = float(shipData[x - 1][self.d_lon])
                    time1 = shipData[x][self.d_timestamp]
                    time2 = shipData[x - 1][self.d_timestamp]

                    dDelta = self.haversine(lat1, lon1, lat2, lon2)
                    tDelta = time1 - time2

                    if dDelta:
                        self.c.execute('UPDATE vessel_data SET distanceDelta = ? WHERE mmsi = ? AND timestamp = ?',
                                       (dDelta, mmsi, time1,))
                        # print("Check | ID: " + str(ide) + " | DDElta: " + str(dDelta))
                    else:
                        self.c.execute('UPDATE vessel_data SET distanceDelta = 0 WHERE mmsi = ? AND timestamp = ?',
                                       (mmsi, time1,))
                        # print("dDelta: " + str(dDelta) + " with lat1: " + str(lat1) + " and lat2: " + str(lat2) + " and lon1: " + str(lon1) + " and lon2: " + str(lon2))
                    if tDelta:
                        self.c.execute('UPDATE vessel_data SET timeDelta = ? WHERE mmsi = ? AND timestamp = ?',
                                       (tDelta, mmsi, time1,))
                        # print("Check | ID: " + str(ide) + " | TDElta: " + str(tDelta))
                    else:
                        self.c.execute('UPDATE vessel_data SET timeDelta = 0 WHERE mmsi = ? AND timestamp = ?',
                                       (mmsi, time1,))
                        # print("tDelta: " + str(tDelta) + " with time1: " + str(time1) + " and time2: " + str(time2))

                if x + 1 > len(shipData):
                    print("End of ship-data! " + str(x) + " Length of Data of this Ship: " + str(len(shipData)))
        self.conn.commit()
        print("[XXXXX        ] Calculated Deltas!")


    def haversine(self, lat1, lon1, lat2, lon2):
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        distance = round(
            6371 * (2 * asin(sqrt(sin((lat2 - lat1) / 2) ** 2 + cos(lat1) * cos(lat2) * sin((lon2 - lon1) / 2) ** 2))),
            5)
        return distance

    def calculate_speed(self):
        self.c.execute('SELECT distinct mmsi FROM vessel_data')
        allShips = self.c.fetchall()

        # for every ship in the database (sorted after time)
        for ship in allShips:
            self.c.execute('SELECT * FROM vessel_data WHERE mmsi = ? ORDER BY mmsi ASC, timestamp ASC', (ship[0],))
            shipData = self.c.fetchall()

            # For every data Point for that mmsi
            for x in range(len(shipData)):
                distanceDelta = shipData[x][self.d_distanceDelta]
                timeDelta = shipData[x][self.d_timeDelta]
                if timeDelta is not None and timeDelta > 0 and distanceDelta is not None:
                    # Distance in km | time in s -> Distance / Time = Speed in km/s | e.g. 0,1km / 1s = 0,1 km/s = 360 km/h / 1.852 = 194,3844492 Knots
                    calculatedSpeed = round(((float(distanceDelta) / float(timeDelta)) * 3600) / 1.852, 2)
                    self.c.execute('UPDATE vessel_data SET calculatedSpeed = ? WHERE mmsi = ? AND timestamp = ?',
                                   (calculatedSpeed, shipData[x][self.d_mmsi], shipData[x][self.d_timestamp],))
                    # print("Calculated Speed " + str(calculatedSpeed))
                else:
                    logging.warning(
                        "Couldn't calculate speed: " + str(shipData[x][self.d_speed]) + " | Timestamp: " + str(
                            shipData[x][self.d_timestamp]))
                    self.c.execute('UPDATE vessel_data SET calculatedSpeed = 0 WHERE mmsi = ? AND timestamp = ?',
                                   (shipData[x][self.d_mmsi], shipData[x][self.d_timestamp],))
        self.conn.commit()
        print("[XXXXXX       ] Calculated Speed!")

    def add_bathymetry(self):
        self.c.execute('SELECT distinct mmsi FROM vessel_data')
        allShips = self.c.fetchall()

        # open the bathymetry map
        dat = rasterio.open('Data/bathymetry.tif')
        z = dat.read()[0]

        # for every ship in the database (sorted after time)
        for ship in allShips:
            self.c.execute('SELECT * FROM vessel_data WHERE mmsi = ? ORDER BY mmsi ASC, timestamp ASC', (ship[0],))
            shipData = self.c.fetchall()

            # For every data Point for that mmsi
            for x in range(len(shipData)):
                # bathymetry
                idx = dat.index(shipData[x][self.d_lon], shipData[x][self.d_lat])
                waterDepth = dat.xy(*idx), z[idx]
                self.c.execute('UPDATE vessel_data SET waterDepth = ? WHERE mmsi = ? AND timestamp = ?',
                               (float(waterDepth[1]), shipData[x][self.d_mmsi], shipData[x][self.d_timestamp]))
        self.conn.commit()
        print("[XXXXXXXXX    ] Added Bathymetry!")


    def adjust_speed(self):
        # Get all mmsi numbers
        self.c.execute('SELECT distinct mmsi FROM vessel_data')
        allShips = self.c.fetchall()

        # For every ship in the database (sorted after time) get all the data
        for ship in allShips:
            self.c.execute('SELECT * FROM vessel_data WHERE mmsi = ? ORDER BY mmsi ASC, timestamp ASC', (ship[0],))
            shipData = self.c.fetchall()

            # Get all speed values from that mmsi
            values_speed = [row[self.d_speed] for row in shipData]

            # calculate mean and standard deviation for the speed values of that mmsi
            mean_speed = np.mean(values_speed)
            std_speed = np.std(values_speed)

            # Delete all entries, that don't satisfy the standard deviation
            self.c.execute('''
                DELETE FROM vessel_data WHERE ABS(speed - ?) > ? * ? AND mmsi = ?
            ''', (mean_speed, self.max_std, std_speed, ship[0],))

            self.conn.commit()
        print("[XXXXXXX      ] Cleaned & normalized speed!")

    def delete_lat_lon(self):
        cleaner_counter = 0

        self.c.execute('SELECT distinct mmsi FROM vessel_data')
        allShips = self.c.fetchall()
        # for every ship in the database (sorted after time)
        for ship in allShips:
            self.c.execute('SELECT * FROM vessel_data WHERE mmsi = ? ORDER BY mmsi ASC, timestamp ASC', (ship[0],))
            shipData = self.c.fetchall()

            for x in range(len(shipData)):
                if self.distance_lat_lon_cleaning:
                    if shipData[x][self.d_distanceDelta] and shipData[x][self.d_distanceDelta] > self.distance_threshold:
                        self.c.execute('DELETE FROM vessel_data WHERE mmsi = ? AND timestamp = ?',
                                       (shipData[x][self.d_mmsi], shipData[x][self.d_timestamp],))
                        cleaner_counter += 1
                        # logging.info("Deleted ID: " + str(shipData[x][self.d]) + " because speed was: " + str(
                        #    shipData[x][5]) + " \n" + str(shipData[x]) + "\n")
                if self.time_lat_lon_cleaning:
                    if shipData[x][self.d_timeDelta] and shipData[x][self.d_timeDelta] > self.time_threshold:
                        self.c.execute('DELETE FROM vessel_data WHERE mmsi = ? AND timestamp = ?',
                                       (shipData[x][self.d_mmsi], shipData[x][self.d_timestamp],))
                        cleaner_counter += 1
                        # logging.info("Deleted ID: " + str(shipData[x][self.d]) + " because speed was: " + str(
                        #    shipData[x][5]) + " \n" + str(shipData[x]) + "\n")
                if self.speed_lat_lon_cleaning:
                    if shipData[x][self.d_calculatedSpeed] and shipData[x][self.d_calculatedSpeed] > self.speed_threshold:
                        self.c.execute('DELETE FROM vessel_data WHERE mmsi = ? AND timestamp = ?',
                                       (shipData[x][self.d_mmsi], shipData[x][self.d_timestamp],))
                        cleaner_counter += 1
                        # logging.info("Deleted ID: " + str(shipData[x][self.d]) + " because speed was: " + str(
                        #    shipData[x][5]) + " \n" + str(shipData[x]) + "\n")
        self.conn.commit()
        print("Cleaned: " + str(cleaner_counter) + " entries because coordinates.")
        print("[XXXXXXXX     ] Deleted unplausible coordinates!")


    # Use Cubic Spline Algorithmn to interpolate between records, if there is a specified time difference between records.
    def interpolation_rec(self):
        # Idee1: Rekursiv den mittleren Punkt zwischen zwei punkten berechnen aus speed und course
        # Idee2: Polynomielle Kurve von Menge1 zu Menge 2 ziehen. Ohne Speed & Kurs
        self.c.execute('SELECT distinct mmsi FROM vessel_data')
        allShips = self.c.fetchall()
        # for every ship in the database (sorted after time)
        for ship in allShips:
            self.c.execute('SELECT * FROM vessel_data WHERE mmsi = ? ORDER BY mmsi ASC, timestamp ASC', (ship[0],))
            shipData = self.c.fetchall()
            for x in range(len(shipData)):
                time_delta = shipData[x][self.d_timeDelta]
                if time_delta and time_delta < self.interpolation_upper_threshold and time_delta > self.interpolation_lower_threshold:
                    if (x - 1) >= 0:
                        values_lat = [shipData[x - 1][self.d_lat], shipData[x][self.d_lat]]
                        values_lon = [shipData[x - 1][self.d_lon], shipData[x][self.d_lon]]
                        values_time = [shipData[x - 1][self.d_timestamp], shipData[x][self.d_timestamp]]

                        latitude = np.array(values_lat)
                        longitude = np.array(values_lon)
                        timestamp = np.array(values_time)

                        # how many interpolation steps? Goal = 1min
                        time_difference_min = int(time_delta / 60)

                        # create cubic spline interpolation for longitude and latitude
                        cs_longitude = CubicSpline(timestamp, longitude)
                        cs_latitude = CubicSpline(timestamp, latitude)

                        # evaluate the cubic spline interpolation at new timestamps
                        new_timestamps = np.linspace(timestamp[0], timestamp[1], num=time_difference_min)
                        new_latitude = cs_latitude(new_timestamps)
                        new_longitude = cs_longitude(new_timestamps)
                        for new_record in range(1, len(new_timestamps)-1):
                            try:
                                self.c.execute(
                                    f"INSERT INTO vessel_data (mmsi, timestamp, lat, lon) VALUES (?, ?, ?, ?)",
                                    (shipData[x][self.d_mmsi], round(new_timestamps[new_record]), round(new_latitude[new_record],5), round(new_longitude[new_record],5)))
                            except Exception as e:
                                logging.error("Entry abundant: " + str(e))
        self.conn.commit()

    def anchorage_detection(self):
        # Get every distinct ship id from the database
        self.c.execute('SELECT distinct mmsi FROM vessel_data')
        allShips = self.c.fetchall()

        # for every ship in the database (sorted after time)
        for ship in allShips:
            self.c.execute('SELECT * FROM vessel_data WHERE mmsi = ? ORDER BY mmsi ASC, timestamp ASC', (ship[0],))
            shipData = self.c.fetchall()

            # Calculate which coordinates is the anchorage point at
            self.c.execute(
                'SELECT * FROM vessel_data WHERE mmsi = ? AND distance_from_shore <= ? ORDER BY mmsi ASC, timestamp ASC',
                (ship[0], self.shore_threshold,))
            anchorage_data = self.c.fetchall()
            values_lat = [row[self.d_lat] for row in anchorage_data]
            values_lon = [row[self.d_lon] for row in anchorage_data]
            # Performance Mode will just calculate one Anchorage at maximum. Without Performance mode all records near
            # shore will be clustered and a anchorage will be calculate per cluster.
            if self.performanceMode:
                lat_lon = list(zip(values_lat, values_lon))
                # Thats the anchorage point in most cases
                most_common = max(set(lat_lon), key=lat_lon.count)
                lat_A = most_common[0]
                lon_A = most_common[1]
                # For every data Point for that mmsi
                for x in range(len(shipData)):
                    # only do that if possible (is not the first data entry for mmsi)
                    if (x - 1) >= 0:
                        # get parameters
                        mmsi = shipData[x][self.d_mmsi]
                        lat1 = float(shipData[x][self.d_lat])
                        lon1 = float(shipData[x][self.d_lon])
                        time1 = shipData[x][self.d_timestamp]
                        aDelta = self.haversine(lat1, lon1, lat_A, lon_A)
                        if aDelta:
                            self.c.execute('UPDATE vessel_data SET anchorageDelta = ? WHERE mmsi = ? AND timestamp = ?',
                                           (aDelta, mmsi, time1,))
                        else:
                            self.c.execute('UPDATE vessel_data SET anchorageDelta = 0 WHERE mmsi = ? AND timestamp = ?',
                                           (mmsi, time1,))

                    if x + 1 > len(shipData):
                        print("End of ship-data! " + str(x) + " Length of Data of this Ship: " + str(len(shipData)))
            else:
                x = np.array(values_lat)
                y = np.array(values_lon)

                zweiD = np.column_stack((x, y))

                try:
                    db = DBSCAN(eps=self.distance_threshold / 6371., min_samples=5, algorithm='ball_tree',
                                metric='haversine').fit(
                        np.radians(zweiD))
                    result = db.labels_
                    lat_lon = list(zip(values_lat, values_lon))
                    sup_package = list(zip((lat_lon), result))

                    # Thats the anchorage point in most cases
                    # Create a dictionary to store the counts of (x,y) for each z
                    counts = defaultdict(Counter)
                    # Count the occurrences of each (x,y) for each z
                    for (x, y), z in sup_package:
                        counts[z].update([(x, y)])
                    # Find the most common (x,y) for each z
                    result = {z: xy.most_common(1)[0][0] for z, xy in counts.items()}

                except Exception as e:
                    logging.warning("Problem: " + str(e))
                    lat_lon = list(zip(values_lat, values_lon))
                    most_common = max(set(lat_lon), key=lat_lon.count)
                    result = {0: most_common}

                # For every data Point for that mmsi
                for x in range(len(shipData)):
                    # only do that if possible (is not the first data entry for mmsi)
                    mmsi = shipData[x][self.d_mmsi]
                    lat1 = float(shipData[x][self.d_lat])
                    lon1 = float(shipData[x][self.d_lon])
                    time1 = shipData[x][self.d_timestamp]

                    aDelta = sys.maxsize
                    anchorage_list = list(result.values())
                    for anchorage in anchorage_list:
                        to_anchorage = self.haversine(lat1, lon1, anchorage[0], anchorage[1])
                        if to_anchorage < aDelta:
                            aDelta = to_anchorage
                    if aDelta < sys.maxsize:
                        self.c.execute('UPDATE vessel_data SET anchorageDelta = ? WHERE mmsi = ? AND timestamp = ?',
                                       (aDelta, mmsi, time1,))
                    else:
                        self.c.execute('UPDATE vessel_data SET anchorageDelta = 0 WHERE mmsi = ? AND timestamp = ?',
                                       (mmsi, time1,))

                    if x + 1 > len(shipData):
                        print("End of ship-data! " + str(x) + " Length of Data of this Ship: " + str(len(shipData)))

        self.conn.commit()
        print("[XXXXXXXXXX   ] Anchorages & deltas detected!")

    def trip_detection(self):
        # Idee: Segmentate ais data into single trips from port to port
        self.c.execute('SELECT distinct mmsi FROM vessel_data')
        allShips = self.c.fetchall()
        # for every ship in the database (sorted after time)
        for ship in allShips:
            self.c.execute('SELECT * FROM vessel_data WHERE mmsi = ? ORDER BY mmsi ASC, timestamp ASC', (ship[0],))
            shipData = self.c.fetchall()

            is_in_anchorage = False
            trip_Count = 0
            for x in range(len(shipData)):
                try:
                    if shipData[x][self.d_anchorageDelta] <= self.anchorage_threshold:
                        is_in_anchorage = True
                        self.c.execute('UPDATE vessel_data SET tripCount = 0 WHERE mmsi = ? AND timestamp = ?',
                                       (shipData[x][self.d_mmsi], shipData[x][self.d_timestamp]))
                    if shipData[x][self.d_anchorageDelta] > self.anchorage_threshold and is_in_anchorage == True:
                        is_in_anchorage = False
                        trip_Count += 1
                        self.c.execute('UPDATE vessel_data SET tripCount = ? WHERE mmsi = ? AND timestamp = ?',
                                       (trip_Count, shipData[x][self.d_mmsi], shipData[x][self.d_timestamp]))
                    else:
                        self.c.execute('UPDATE vessel_data SET tripCount = ? WHERE mmsi = ? AND timestamp = ?',
                                       (trip_Count, shipData[x][self.d_mmsi], shipData[x][self.d_timestamp]))
                except Exception as e:
                    logging.warning("anchorageDelta is none!")
        self.conn.commit()
        print("[XXXXXXXXXXX  ] Trips Detected!")

    def export_database(self):
        all_data = self.c.execute("SELECT * FROM vessel_data")
        with open('Data/toclean/cleaned_ais_data.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['mmsi','timestamp','distance_from_shore','distance_from_port','speed','calculatedSpeed','course','lat','lon','is_fishing','source','dataset','timestampR','distanceDelta','timeDelta','anchorageDelta','waterDepth','tripCount'])
            writer.writerows(all_data)
        print("[XXXXXXXXXXXX ] Database exported as csv")


test = Pipeline(fishing_types, dma)