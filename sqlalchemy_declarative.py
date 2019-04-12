import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String, Boolean, Numeric, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine

Base = declarative_base()

db_folder_path = 'database'
 
class Trip(Base):
    __tablename__ = 'trip'
    id = Column(Integer, primary_key=True)
    lng_start = Column(String(40), nullable=False)
    lat_start = Column(String(40), nullable=False)
    lng_end = Column(String(40), nullable=False)
    lat_end = Column(String(40), nullable=False)
    date_start = Column(Date, nullable=False)
    date_end = Column(Date, nullable=False)
    time_start = Column(Integer, nullable=False)
    time_end = Column(Integer, nullable=False)
    distance = Column(Integer, nullable=False)
    trip_duration = Column(Integer, nullable=False)
    is_distance_time_matched = Column(Boolean, unique=False, default=False)
    is_complete = Column(Boolean, unique=False, default=False)
 
class Route(Base):
    __tablename__ = 'route'
    id = Column(Integer, primary_key=True)
    lng_start = Column(String(40), nullable=False)
    lat_start = Column(String(40), nullable=False)
    lng_end = Column(String(40), nullable=False)
    lat_end = Column(String(40), nullable=False)
    distance = Column(Integer)
    trip_duration = Column(Integer, nullable=False)
    time_slot = Column(Integer, nullable=False)

class Trajectory(Base):
    __tablename__ = 'trajectory'
    id = Column(Integer, primary_key=True)
    lng_start = Column(String(40), nullable=False)
    lat_start = Column(String(40), nullable=False)
    lng_end = Column(String(40), nullable=False)
    lat_end = Column(String(40), nullable=False)
    distance = Column(Integer)
    trip_duration = Column(Integer, nullable=False)
    is_complete = Column(Boolean, unique=False, default=False)

class RouteTrajectory(Base):
    __tablename__ = 'route_trajectory'
    id = Column(Integer , primary_key=True)
    route_id = Column(Integer, ForeignKey('route.id'))
    trajectory_id = Column(Integer, ForeignKey('trajectory.id'))
    route = relationship(Route)
    trajectory = relationship(Trajectory)

class TripRoute(Base):
    __tablename__ = 'trip_route'
    id = Column(Integer, primary_key=True)
    trip_id = Column(Integer, ForeignKey('trip.id'))
    route_id = Column(Integer, ForeignKey('route.id'))
    trip = relationship(Trip)
    route = relationship(Route)

class TrajectoryPoint(Base):
    __tablename__ = 'trajectory_point'
    id = Column(Integer, primary_key=True)
    lng = Column(String(40), nullable=False)
    lat = Column(String(40), nullable=False)
    trajectory_id = Column(Integer, ForeignKey(Trajectory.id))
    trajectory = relationship(Trajectory)

# Create an engine that stores data in the local directory's
if not os.path.exists(db_folder_path):
    os.makedirs(db_folder_path)
engine = create_engine('sqlite:///'+db_folder_path+'/chicago_trips.db')
 
# Create all tables in the engine. This is equivalent to "Create Table"
# statements in raw SQL.
Base.metadata.create_all(engine)