from flask import Flask, request, Response
from database import shared_session
from models.stoppage import Stoppage
from models.threshold import Threshold
from models.data_event import DataEvent
from models.token import Token

import paho.mqtt.client as mqtt
import json


db_session = shared_session

broker_address = "127.0.0.1"
port = 1883
topic = "ibisa/stream" 

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conexión exitosa al servidor MQTT")
    else:
        print("Error de conexión al servidor MQTT, código:", rc)

client = mqtt.Client()
client.on_connect = on_connect

client.connect(broker_address, port)


app = Flask(__name__)

@app.route("/api/stoppage", defaults={ '_id': None }, methods=['GET', 'POST'])
@app.route("/api/stoppage/<_id>", methods=['GET', 'POST', 'PUT', 'DELETE'])
def stoppage(_id):
    if request.method == 'POST':
        body = request.get_json()
        stoppage = Stoppage(**body)
        db_session.add(stoppage)
        db_session.commit()
        db_session.flush()
        client.publish(topic, f"stoppage")
        return Response(status=201)
    elif request.method == 'PUT':
        stoppage = db_session.query(Stoppage).get(_id)
        if stoppage is not None:
            body = request.get_json()
            for property in body:
                setattr(stoppage, property, body[property])
            db_session.commit()
            db_session.flush()
            client.publish(topic, f"stoppage")
            return Response()
        return Response(status=404)
    elif request.method == 'DELETE':
        stoppage = db_session.query(Stoppage).filter_by(id=_id)
        if stoppage is not None:
            stoppage.delete()
            db_session.commit()
            return Response(status=204)
        return Response(status=404)
    elif request.method == 'GET':
        if _id is None:
            stoppages = db_session.query(Stoppage).all()
            data_json = [
                { 'id': res.id, 'name': res.name, 'metric': res.metric, 'limit_': res.limit_, 'to_event': res.to_event } 
                for res in stoppages]
            return Response(response=json.dumps(data_json), headers={ 'Content-Type': 'application/json' })
        else:
            stoppage = db_session.query(Stoppage).get(_id)
            if stoppage is not None:
                data_json = { 'id': stoppage.id, 'name': stoppage.name, 'metric': stoppage.metric, 'limit_': stoppage.limit_, 'to_event': stoppage.to_event } 
                return Response(response=json.dumps(data_json), headers={ 'Content-Type': 'application/json' })
            return Response(status=404)

@app.route("/api/threshold", defaults={ '_id': None }, methods=['GET', 'POST'])
@app.route("/api/threshold/<_id>", methods=['GET', 'POST', 'PUT', 'DELETE'])
def threshold(_id):
    if request.method == 'POST':
        body = request.get_json()
        threshold = Threshold(**body)
        db_session.add(threshold)
        db_session.commit()
        db_session.flush()
        client.publish(topic, f"threshold")
        return Response(status=201)
    elif request.method == 'PUT':
        threshold = db_session.query(Threshold).get(_id)
        if threshold is not None:
            body = request.get_json()
            for property in body:
                setattr(threshold, property, body[property])
            db_session.commit()
            db_session.flush()
            client.publish(topic, f"threshold")
            return Response()
        return Response(status=404)
    elif request.method == 'DELETE':
        threshold = db_session.query(Threshold).filter_by(id=_id)
        if threshold is not None:
            threshold.delete()
            db_session.commit()
            return Response(status=204)
        return Response(status=404)
    elif request.method == 'GET':
        if _id is None:
            thresholds = db_session.query(Threshold).all()
            data_json = [
                { 'id': res.id, 'name': res.name, 'metric': res.metric, 'limit_': res.limit_, 'type_': res.type_, 'to_event': res.to_event } 
                for res in thresholds]
            return Response(response=json.dumps(data_json), headers={ 'Content-Type': 'application/json' })
        else:
            threshold = db_session.query(Threshold).get(_id)
            if threshold is not None:
                data_json = { 'id': threshold.id, 'name': threshold.name, 'metric': threshold.metric, 'type_': threshold.type_, 'limit_': threshold.limit_, 'to_event': threshold.to_event } 
                return Response(response=json.dumps(data_json), headers={ 'Content-Type': 'application/json' })
            return Response(status=404)


@app.route("/api/data", defaults={ '_id': None }, methods=['GET', 'POST'])
@app.route("/api/data/<_id>", methods=['GET', 'POST', 'PUT', 'DELETE'])
def data(_id):
    if request.method == 'POST':
        body = request.get_json()
        data_event = DataEvent(**body)
        db_session.add(data_event)
        db_session.commit()
        db_session.flush()
        client.publish(topic, f"data_event")
        return Response(status=201)
    elif request.method == 'PUT':
        data_event = db_session.query(DataEvent).get(_id)
        if data_event is not None:
            body = request.get_json()
            for property in body:
                setattr(data_event, property, body[property])
            db_session.commit()
            db_session.flush()
            client.publish(topic, f"data_event")
            return Response()
        return Response(status=404)
    elif request.method == 'DELETE':
        data_event = db_session.query(DataEvent).filter_by(id=_id)
        if data_event is not None:
            data_event.delete()
            db_session.commit()
            return Response(status=204)
        return Response(status=404)
    elif request.method == 'GET':
        if _id is None:
            data_events = db_session.query(DataEvent).all()
            data_json = [
                { 'id': res.id, 'name': res.name, 'metric': res.metric, 'limit_': res.limit_, 'to_event': res.to_event } 
                for res in data_events]
            return Response(response=json.dumps(data_json), headers={ 'Content-Type': 'application/json' })
        else:
            data_event = db_session.query(DataEvent).get(_id)
            if data_event is not None:
                data_json = { 'id': data_event.id, 'name': data_event.name, 'metric': data_event.metric, 'limit_': data_event.limit_, 'to_event': data_event.to_event } 
                return Response(response=json.dumps(data_json), headers={ 'Content-Type': 'application/json' })
            return Response(status=404)


@app.route("/api/token", methods=['POST'])
def token():
    if request.method == 'POST':
        token = db_session.query(Token).first()
        if token is not None:
            body = request.get_json()
            for property in body:
                setattr(token, property, body[property])
            db_session.commit()
            db_session.flush()
            return Response(status=200)
        else:
            body = request.get_json()
            token = Token(**body)
            db_session.add(token)
            db_session.commit()
            db_session.flush()
            return Response(status=201)


if __name__ == "__main__":
    app.run()