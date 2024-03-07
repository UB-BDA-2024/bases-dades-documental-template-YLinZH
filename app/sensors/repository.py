from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.redis_client import RedisClient
from app.mongodb_client import MongoDBClient
import json


from . import models, schemas, last_data

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb_client: MongoDBClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    mydoc = {
        "sensor_id": db_sensor.id,
        "location": {
            "type": "Point",
            "coordinates": [sensor.longitude, sensor.latitude]
        },
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version
        }

    mongodb_client.insertOne(mydoc)
    return db_sensor

def record_data(redis: RedisClient, sensor_id: int, data: schemas.SensorData, db: Session, mongodb_client: MongoDBClient) -> schemas.Sensor:
    #obtenir sensor si existeix
    db_sensor = get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    serialized_data = json.dumps(data.dict())

    sensor_document = get_sensor_document(sensor_id=sensor_id, mongodb_client=mongodb_client)
    if sensor_document is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    
    # guardar noves dades en la base de dades
    redis.set(sensor_id, serialized_data)
    sensor = schemas.Sensor(id=sensor_id, name=db_sensor.name, 
                            latitude=sensor_document["location"]["coordinates"][0], longitude=sensor_document["location"]["coordinates"][1],
                            type=sensor_document["type"], mac_address=sensor_document["mac_address"],
                            joined_at=db_sensor.joined_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), temperature=data.temperature,
                            velocity=data.velocity, humidity=data.humidity, battery_level=data.battery_level, last_seen=data.last_seen)
    return sensor

def get_data(redis: RedisClient, sensor_id: int, db: Session, mongodb_client: MongoDBClient) -> schemas.Sensor:
    
    #obtenir les dades des de PostgreSQL i Redis si existeixen
    db_sensordata_bytes = redis.get(sensor_id)
    if db_sensordata_bytes is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db_sensor = get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    
    # obtenir les dades de Redis
    db_sensordata = schemas.SensorData.parse_raw(db_sensordata_bytes)
    sensor_document = get_sensor_document(sensor_id=sensor_id, mongodb_client=mongodb_client)
    # Retorna un sensor complert
    sensor = schemas.Sensor(id=sensor_id, name=db_sensor.name, 
                            latitude=sensor_document["location"]["coordinates"][0], longitude=sensor_document["location"]["coordinates"][1],
                            type=sensor_document["type"], mac_address=sensor_document["mac_address"],
                            joined_at=db_sensor.joined_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), temperature=db_sensordata.temperature, 
                            velocity=db_sensordata.velocity,humidity=db_sensordata.humidity, battery_level=db_sensordata.battery_level, last_seen=db_sensordata.last_seen)

    return sensor

def get_sensor_document(sensor_id: int, mongodb_client: MongoDBClient):
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    x = mongodb_client.findOne({"sensor_id": sensor_id})
    # convert ObjectId to string for JSON serialization
    if x:
        x["_id"] = str(x["_id"])
        return x
    return None

def get_sensors_near(latitude: float, longitude: float, radius: int, db: Session, mongodb_client: MongoDBClient, redis_client: RedisClient):
    mongodb_client.getDatabase("sensors")
    collection = mongodb_client.getCollection("sensorsCol")
    collection.create_index([("location", "2dsphere")])
    geoJSON = {
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                },
                "$maxDistance": radius
            }
        }
    }
    result = mongodb_client.findAllDocuments(geoJSON)
    # Convert MongoDB cursor to a list of dictionaries
    nearby_sensors = list(result)
    sensors = []
    # Optionally, convert ObjectId to string for JSON serialization
    for doc in nearby_sensors:
        doc["_id"] = str(doc["_id"])
        sensor = get_data(redis=redis_client, sensor_id=doc["sensor_id"], db=db, mongodb_client=mongodb_client)
        if sensor is not None:
            sensors.append(sensor)
    
    if sensors is not None:
        return sensors
    return []

def delete_sensor(db: Session, sensor_id: int, mongodb_client: MongoDBClient):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    mongodb_client.deleteOne({"sensor_id": sensor_id})

    db.delete(db_sensor)
    db.commit()
    return db_sensor