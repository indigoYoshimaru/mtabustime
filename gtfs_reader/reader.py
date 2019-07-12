from pymongo import MongoClient
from google.transit import gtfs_realtime_pb2
import pprint

data_file=open("bus.gtfs",'rb') #use rb to read binary
feed=gtfs_realtime_pb2.FeedMessage()

feed.ParseFromString(data_file.read())

#pprint.pprint(feed)

mclient=MongoClient('localhost') #connect to database server
db=mclient.gtfs     #get the database
db.vehicle      #get the collection

def entity_to_dict (ent):
    result=dict()
    result['id']=ent.id
    result['vehicle']=dict()
    result['vehicle']['id']=ent.id
    result['vehicle']['trip']=dict()
    result['vehicle']['trip']['trip_id']=ent.vehicle.trip.trip_id
    result['vehicle']['trip']['startdate']=ent.vehicle.trip.start_date
    result['vehicle']['trip']['route_id']=ent.vehicle.trip.route_id
    result['vehicle']['trip']['direction']=ent.vehicle.trip.direction_id
    result['vehicle']['position']=dict()
    #result['vehicle']['position']['position_id']=ent.vehicle.position.position_id
    result['vehicle']['position']['longitude']=ent.vehicle.position.longitude
    result['vehicle']['position']['latitude']=ent.vehicle.position.latitude
    result['vehicle']['timestamp']=ent.vehicle.timestamp
    result['vehicle']['stop']=ent.vehicle.stop_id


    #result['stop']=dict()
    #result['stop']['id']=ent.stop_id
    #result['stop']['longitude']=ent.stop.longitude
    #result['stop']['latitude']=ent.stop.latitude

    return result

for entity in feed.entity:
    d=entity_to_dict(entity)    #convert object to dictionary

    db.vehicle.insert_one(d)

    




