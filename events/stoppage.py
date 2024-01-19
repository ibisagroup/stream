import sched
import time
import concurrent.futures
import paho.mqtt.client as mqtt

from database import shared_session

from models.data import Data
from models.stoppage import Stoppage
from utilities.constanst import HOURS

db_session = shared_session
broker_address = "mosquitto"
port = 1883
topic = "devteam/888888/stream"
topic_event = "ibisa/stream"

# Stoppage: deja de contar por un rango de tiempo
stoppages: list[Stoppage] = []

scheduler = sched.scheduler(time.time, time.sleep)

is_task_current = False


def load_stoppages():
    global stoppages, is_task_current
    actives = []
    db_session.expire_all()
    stoppages_ = db_session.query(Stoppage).filter_by(is_active=True).all()
    for stoppage_ in stoppages_:
        actives.append(stoppage_.id)
        stoppages_match = list(filter(lambda item: item.id == stoppage_.id, stoppages))
        stoppage: Stoppage = stoppages_match[0] if len(stoppages_match) > 0 else None
        if stoppage is None:
            new_stoppage = stoppage_.__dict__
            new_stoppage.pop('_sa_instance_state')
            new_stoppage.pop('is_active')
            stoppages.append(Stoppage(**new_stoppage))
        else:
            for property in stoppage.__dict__:
                if property in stoppage_.__dict__:
                    setattr(stoppage, property, getattr(stoppage_, property))
    if not is_task_current:
        scheduler.enter(HOURS * 0.01, 1, load_stoppages, ())
        is_task_current = True
    stopagges = [stoppage for stoppage in stoppages if stoppage.id in actives]
    print(f'Eventos activos stoppages: {stopagges}')
    is_task_current = False

load_stoppages()

def on_connect(client, userdata, flags, rc):
    print("Code result connection from mqtt: " + str(rc))
    client.subscribe(topic_event)
    client.subscribe(topic)

def on_message(client, userdata, message):
    payload = message.payload.decode()
    if message.topic == topic_event and payload == "stoppage":
        load_stoppages()
    if message.topic == topic:
        future = executor.submit(detect_event, payload)
        future.add_done_callback(result_process)

def detect_event(payload):
    global stoppages
    data = str(payload)
    data_arr = data.split(";")[:-1]
    events_log = []
    if len(stoppages):
        for data_ in data_arr:
            data_self = data_.split(",")
            data_obj: Data = Data(int(data_self[0]), data_self[1], data_self[2])
            for stoppage in stoppages:
                is_owner: bool = stoppage.metric == data_obj.name
                if is_owner:
                    if stoppage.last_data is None:
                        stoppage.last_data = data_obj
                        continue
                    else:
                        stoppage.current_data = data_obj

                    stoppage.canTriggerEvent()
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