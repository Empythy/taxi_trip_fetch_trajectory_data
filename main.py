import os
from sqlalchemy_declarative import Trip, Route, Trajectory, RouteTrajectory, TripRoute, TrajectoryPoint, Base
from sqlalchemy import create_engine, desc, func
from datetime import date
db_folder_path = 'database'
engine = create_engine('sqlite:///'+db_folder_path+'/chicago_trips.db')
Base.metadata.bind = engine
from sqlalchemy.orm import sessionmaker
DBSession = sessionmaker()
DBSession.bind = engine
session = DBSession()

import pandas as pd
infile = 'Dataset/cleaned_taxi_trip_updated_date_2017_q1.csv'
chunksize = 100000

import googlemaps
import private.keys as pk
gmaps = googlemaps.Client(key=pk.gmap_key)


distance_tolerance = 0.1
trip_duration_tolerance = 0.2

def add_trip_to_db(trip_row):
  new_trip = Trip(lng_start=trip_row['Pickup Centroid Longitude'], lat_start=trip_row['Pickup Centroid Latitude'], lng_end=trip_row['Dropoff Centroid Longitude'], lat_end=trip_row['Dropoff Centroid Latitude'], date_start=trip_row['date_start'], date_end=trip_row['date_end'], time_start=trip_row['time_start'], time_end=trip_row['time_end'], distance = trip_row['Trip Miles']*1609.344, trip_duration = trip_row['Trip Seconds'])
  session.add(new_trip)

def add_trajectory_to_db(step):
  new_trajectory = Trajectory(lng_start = step["start_location"]["lng"], lat_start = step["start_location"]["lat"], lng_end = step["end_location"]["lng"] , lat_end = step["end_location"]["lat"], distance = step["distance"]["value"], trip_duration = step["duration"]["value"] )
  session.add(new_trajectory)
  session.commit()
  return new_trajectory

def add_route_trajectory_to_db(route, trajectory):
  new_route_trajectory = RouteTrajectory(route = route, trajectory = trajectory)
  session.add(new_route_trajectory)
  session.commit()
  return new_route_trajectory

def add_route_to_db(route, trip, time_slot):
  route = route["legs"][0]
  new_route = Route(lng_start= trip.lng_start, lat_start=trip.lat_start, lng_end=trip.lng_end, lat_end=trip.lat_end, distance=route["distance"]["value"], trip_duration=route["duration"]["value"], time_slot=time_slot)
  session.commit()
  steps = route["steps"]
  for step in steps:
    trajectory = store_or_get_id_of_trajectory(step)
    add_route_trajectory_to_db(new_route, trajectory)
  return new_route
    

def update_trip_to_complete(trip, is_distance_time_matched = False):
  trip.is_complete = True
  trip.is_distance_time_matched = is_distance_time_matched
  session.commit()
  return trip

def display_one_trip(trip):
  print("id: ", trip.id)
  print("lng_start: ", trip.lng_start)
  print("lat_start: ", trip.lat_start)
  print("lng_end: ", trip.lng_end)
  print("lat_end: ", trip.lat_end)
  print("date_start: ", trip.date_start)
  print("date_end: ", trip.date_end)
  print("time_start: ", trip.time_start)
  print("time_end: ", trip.time_end)
  print("distance: ", trip.distance)
  print("trip_duration: ", trip.trip_duration)
  print("is_distance_time_matched: ", trip.is_distance_time_matched)
  print("is_complete: ", trip.is_complete)

def display_one_trajectory(trajectory):
  print("id: ", trajectory.id)
  print("lng_start: ", trajectory.lng_start)
  print("lat_start: ", trajectory.lat_start)
  print("lng_end: ", trajectory.lng_end)
  print("lat_end: ", trajectory.lat_end)
  print("distance: ", trajectory.distance)
  print("time: ", trajectory.time)

def display_trips(trips):
  for trip in trips:
    display_one_trip(trip)

def display_trajectories(trajectories):
  for trajectory in trajectories:
    display_one_trajectory(trajectory)

def read_data_from_csv():
  i = 1
  for df in pd.read_csv(infile, chunksize=chunksize, iterator=True, na_values=['']):
    print('i---------', i)
    i+=1
    df.dropna(inplace=True)
    df['Trip Start Timestamp'] = pd.to_datetime(df['Trip Start Timestamp'], utc=True)
    df['Trip End Timestamp'] = pd.to_datetime(df['Trip End Timestamp'], utc=True)
    start_date = '2017-01-01 00:00:00+00:0'
    end_date = '2017-01-31 23:59:00+00:0'
    mask = (df['Trip Start Timestamp'] >= start_date) & (df['Trip Start Timestamp'] <= end_date)
    df = df.loc[mask]
    df['date_start'] = df['Trip Start Timestamp'].dt.date
    df['time_start'] = df['Trip Start Timestamp'].dt.hour*100 + df['Trip Start Timestamp'].dt.minute
    df['date_end'] = df['Trip End Timestamp'].dt.date
    df['time_end'] = df['Trip End Timestamp'].dt.hour*100 + df['Trip End Timestamp'].dt.minute
    for index, row in df.iterrows():
      add_trip_to_db(row)
    session.commit()

def store_or_get_id_of_trajectory(step):
  trajectory = session.query(Trajectory).filter(Trajectory.lng_start == step["start_location"]["lng"], Trajectory.lat_start == step["start_location"]["lat"], Trajectory.lng_end == step["end_location"]["lng"], Trajectory.lat_end == step["start_location"]["lat"]).first()
  if (not trajectory):
    trajectory = add_trajectory_to_db(step)
  return trajectory

def get_best_matching_route(existing_routes, trip):
  best_route = None
  for route in existing_routes:
    if ((abs(route.distance-trip.distance)/trip.distance)<=distance_tolerance):
      if ((abs(route.trip_duration-trip.trip_duration)/trip.trip_duration)<=trip_duration_tolerance):
        best_route = route
        break
  return best_route


def add_trip_data_from_existing_route(existing_routes, trip):
  best_route = get_best_matching_route(existing_routes, trip)
  is_distance_time_matched = False
  if (best_route):
    new_trip_route = TripRoute(trip = trip, route = best_route)
    session.add(new_trip_route)
    session.commit()
    is_distance_time_matched = True
  update_trip_to_complete(trip, is_distance_time_matched)

def fetch_trajectory_data(trip, time_slot):
  routes = gmaps.directions((trip.lat_start, trip.lng_start),(trip.lat_end, trip.lng_end), alternatives=True)
  db_routes = []
  for route in routes:
    db_routes.append(add_route_to_db(route, trip, time_slot))
  if (len(db_routes)):
      add_trip_data_from_existing_route(db_routes, trip)

def get_and_fill_trajectory_data():
  i = 1
  while True:
    trips = session.query(Trip).filter(Trip.is_complete == False, Trip.distance>0, Trip.trip_duration>0, Trip.date_start<='2017-01-01').order_by(func.abs(Trip.distance)).offset(0).limit(100).all()
    for trip in trips:
      print("i--------:", i)
      i+=1
      time_slot = trip.time_start-trip.time_start%100
      existing_routes = session.query(Route).filter(Route.lng_start == trip.lng_start, Route.lat_start == trip.lat_start, Route.lng_end == trip.lng_end, Route.lat_end == trip.lat_end , Route.time_slot == time_slot).order_by(func.abs(Route.distance-trip.distance), func.abs(Route.trip_duration-trip.trip_duration)).all()
      # print("existing_routes:", existing_routes.statement.compile(compile_kwargs={"literal_binds": True}))
      if (len(existing_routes)):
        add_trip_data_from_existing_route(existing_routes, trip)
      else:
        fetch_trajectory_data(trip, time_slot)
    if (len(trips)==0):
      break


def get_and_fill_trajectory_point_data():
  trajectories = session.query(Trajectory).offset(0).limit(5).all()
  display_trajectories(trajectories)
  # for trajectory in trajectories:

if __name__ == "__main__":
  # read_data_from_csv()
  get_and_fill_trajectory_data()
  # export test='Hassan'
  # get_and_fill_trajectory_point_data()