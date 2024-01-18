import sched
import time
import concurrent.futures
import paho.mqtt.client as mqtt

from database import shared_session

from models.data import Data
from models.threshold import Threshold
from utilities.constanst import HOURS


db_session = shared_session
broker_address = "127.0.0.1"
port = 1883
topic = "devteam/888888/stream"
topic_event = "ibisa/thresholds"

# Threshold: deja de contar por un rango de tiempo
thresholds: list[Threshold] = []

scheduler = sched.scheduler(time.time, time.sleep)

is_task_current = False


def load_thresholds():
    global thresholds, is_task_current
    actives = []
    db_session.expire_all()
    thresholds_ = db_session.query(Threshold).filter_by(is_active=True).all()
    for threshold_ in thresholds_:
        actives.append(threshold_.id)
        thresholds_match = list(filter(lambda item: item.id == threshold_.id, thresholds))
        threshold: Threshold = thresholds_match[0] if len(thresholds_match) > 0 else None
        if threshold is None:
            new_threshold = threshold_.__dict__
            _ = new_threshold.pop('_sa_instance_state')
            _ = new_threshold.pop('is_active')
            thresholds.append(Threshold(**new_threshold))
        else:
            for property in threshold.__dict__:
                if property in threshold_.__dict__:
                    setattr(threshold, property, getattr(threshold_, property))
    if not is_task_current:
        scheduler.enter(HOURS * 0.01, 1, load_thresholds, ())
        is_task_current = True
    thresholds = [threshold for threshold in thresholds if threshold.id in actives]
    print(f'Eventos activos thresholds: {thresholds}')
    is_task_current = False

load_thresholds()

def on_connect(client, userdata, flags, rc):
    print("Code result connection from mqtt: " + str(rc))
    client.subscribe(topic)

def on_message(client, userdata, message):
    payload = message.payload.decode()
    if message.topic == topic_event:
        load_thresholds()
    if message.topic == topic:
        future = executor.submit(detect_event, payload)
        future.add_done_callback(result_process)

def detect_event(payload):
    global thresholds
    data = str(payload)
    data_arr = data.split(";")[:-1]
    events_log = []
    if len(thresholds):
        for data_ in data_arr:
            data_self = data_.split(",")
            data_obj: Data = Data(int(data_self[0]), data_self[1], data_self[2])
            for threshold in thresholds:
                is_owner: bool = threshold.metric == data_obj.name
                if is_owner:
                    if threshold.last_data is None:
                        threshold.last_data = data_obj
                        continue
                    else:
                        threshold.current_data = data_obj

                    threshold.canTriggerEvent()
    return events_log

def result_process(future):
    result = future.result()
    print("Resultado:", result)

client_mqtt = mqtt.Client()
client_mqtt.on_message = on_message
client_mqtt.on_connect = on_connect
client_mqtt.connect(broker_address, port)
client_mqtt.loop_start()

executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

def run():
    try:
        while True:
            scheduler.run()
    except KeyboardInterrupt:
        print("Close connection")
        client_mqtt.loop_stop()
        client_mqtt.disconnect()
        executor.shutdown()