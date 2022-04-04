# run process_data.py... saves final_data.csv in same directory

from tqdm import tqdm

tqdm.pandas()
import pandas as pd
import numpy as np

from porto_utils import *

data_folder_path = "./data"  # based on setup script's download location
# data_folder_path = "~/Documents/porto/data"  # based on setup script's download location

tqdm.write("Loading Data")
csv_name = "%s/train.csv" % data_folder_path
df = pd.read_csv(csv_name)  # 1,710,670 lines...
raw_data = df.sample(frac=0.1, random_state=1)  # frac 1 == 100%... change frac when want to test with less data
tqdm.write("Analyzing %d rows" % len(raw_data))

# converts string of lon lat coordinates to array of lon lat coords... saves at col=lon_lat
tqdm.write("Processing Data")
raw_data['lon_lat'] = raw_data.POLYLINE.progress_apply(lambda x: analy_str_xy(x))
raw_data = raw_data[raw_data['MISSING_DATA'] == False]
raw_data = raw_data[['TAXI_ID', 'lon_lat', 'TIMESTAMP', 'TRIP_ID']]
raw_data = raw_data.explode('lon_lat')  # converts array of lon lats to a row for each lon lat
raw_data = raw_data.drop(raw_data[raw_data['lon_lat'] == ''].index)
raw_data[['lon', 'lat']] = raw_data['lon_lat'].str.split(",", expand=True, )
raw_data[['lon', 'lat']] = raw_data[['lon', 'lat']].apply(pd.to_numeric, errors='raise')  # convert to floats
raw_data['trip_index'] = raw_data.groupby(['TRIP_ID']).cumcount()
raw_data['TIMESTAMP'] = raw_data['TIMESTAMP'].add(raw_data['trip_index'].mul(15))  # increment TS per trip id by 15s
raw_data = raw_data[['TAXI_ID', 'TIMESTAMP', 'lon', 'lat']]

tqdm.write("There are %d taxis." % len(raw_data['TAXI_ID'].unique()))

# sets up camera indexing BUT STILL NOT ENTIRELY SURE WHAT THIS DOES
tqdm.write("Preparing cameras")
Lon_a = raw_data.lon.quantile(0.05)  # not sure why they do this quantile bit.. why not take max min
Lon_b = raw_data.lon.quantile(0.95)
Lat_a = raw_data.lat.quantile(0.05)
Lat_b = raw_data.lat.quantile(0.95)
cam_granularity = 30  # camera number per column
cam_number = cam_granularity
Lon_inter = (Lon_b - Lon_a) / cam_granularity
Lat_inter = (Lat_b - Lat_a) / cam_granularity
new_cam_center_xy = np.array(
    check_overlap(camera_coordinates, Lon_inter, Lat_inter))  # makes sure no 2 cameras overlap!
tqdm.write("%d non overlapping cameras... " % len(new_cam_center_xy))
camap = cam_map_index(new_cam_center_xy, Lon_inter, Lat_inter, Lon_a, Lon_b, Lat_a, Lat_b)


def set_cam_index_manually(x):
    x_lon = x['lon']
    x_lat = x['lat']
    if x_lon <= Lon_a or x_lon >= Lon_b or x_lat <= Lat_a or x_lat >= Lat_b:
        return None
    xindex = int((x_lon - Lon_a) / (1.05 * Lon_inter))
    yindex = int((x_lat - Lat_a) / (1.05 * Lat_inter))
    if camap.get(xindex) and camap[xindex].get(yindex):
        for ci in camap[xindex][yindex]:
            x1 = new_cam_center_xy[ci][0]  # - Lon_inter
            y1 = new_cam_center_xy[ci][1]  # - Lat_inter
            x2 = new_cam_center_xy[ci][0] + Lon_inter
            y2 = new_cam_center_xy[ci][1] + Lat_inter
            if x_lon > x1 and x_lon < x2 and x_lat > y1 and x_lat < y2:
                return ci
    return None


tqdm.write("Identifying camera for each GPS point in each trajectory")
raw_data['cam_index'] = raw_data[['lon', 'lat']].progress_apply(set_cam_index_manually, axis=1)

tqdm.write("Final Clean Up")
final_data = raw_data.dropna()  # remove any points that aren't in view of any cameras
final_data = final_data[['TAXI_ID', 'TIMESTAMP', 'cam_index']]
final_data['CAMERA_ID'] = pd.factorize(final_data['cam_index'])[0]
final_data['TAXI_ID'] = pd.factorize(final_data['TAXI_ID'])[0]
tqdm.write("Final Data has %d rows, %d cameras, and %d taxis" % (
    len(final_data), final_data['CAMERA_ID'].max() + 1, len(final_data['TAXI_ID'].unique())))
final_data = final_data.reset_index()
final_data = final_data.drop(['index', 'cam_index'], axis=1)

# each row: columns: TAXI_ID, TIMESTAMP, CAMERA_ID
# ie. taxi <TAXI_ID> is in view of camera <CAMERA_ID> at <TIMESTAMP> for the next 15 seconds
final_data.to_csv("final_data.csv", index=False)
