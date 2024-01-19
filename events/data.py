from copy import deepcopy
import datetime
import multiprocessing
import sched
import time
import concurrent.futures
import paho.mqtt.client as mqtt

from database import shared_session

from models.data import Data
from models.data_event import DataEvent
from utilities.constanst import HOURS

db_session = shared_session
broker_address = "mosquitto"
port = 1883
topic = "devteam/888888/stream"
topic_event = "ibisa/data_events"
topic_local = "ibisa/data_events/default"

# DataEvent: deja de contar por un rango de tiempo
manager = multiprocessing.Manager()
data_events = manager.list()

scheduler = sched.scheduler(time.time, time.sleep)

is_task_current = False


def load_data_event():
    global data_events, is_task_current
    actives = []
    db_session.expire_all()
    data_events_ = db_session.query(DataEvent).filter_by(is_active=True).all()
    for data_event_ in data_events_:
        actives.append(data_event_.id)
        data_events_match = list(filter(lambda item: item.id == data_event_.id, data_events))
        data_event: DataEvent = data_events_match[0] if len(data_events_match) > 0 else None
        if data_event is None:
            new_data_event = data_event_.__dict__
            _ = new_data_event.pop('_sa_instance_state')
            _ = new_data_event.pop('is_active')
            data_events.append(DataEvent(**new_data_event))
        else:
            for property in data_event.__dict__:
                if property in data_event_.__dict__:
                    setattr(data_event, property, getattr(data_event_, property))
    if not is_task_current:
        scheduler.enter(HOURS * 0.01, 1, load_data_event, ())
        is_task_current = True
    data_events = [data_event for data_event in data_events if data_event.id in actives]
    print(f'Eventos activos data: {data_events}')
    is_task_current = False

load_data_event()

def on_connect(client, userdata, flags, rc):
    print("Code result connection from mqtt: " + str(rc))
    client.subscribe(topic_event)
    client.subscribe(topic)
    client.subscribe(topic_local)

def on_message(client, userdata, message):
    payload = message.payload.decode()
    if message.topic == topic_event:
        load_data_event()
    if message.topic == topic or message.topic == topic_local:
        future = executor.submit(detect_event, payload)
        future.add_done_callback(result_process)

def detect_event(payload):
    global data_events
    data = str(payload)
    data_arr = data.split(";")[:-1]
    events_log = []
    if len(data_events):
        for data_ in data_arr:
            data_self = data_.split(",")
            data_obj: Data = Data(int(data_self[0]), data_self[1], data_self[2])
            for i, data_event in enumerate(data_events):
                is_owner: bool = data_event.metric == data_obj.name
                if is_owner:
                    if data_event.last_data is None:
                        data_event.last_data = data_obj
                        continue
                    else:
                        data_event.current_data = data_obj

                    data_event.canTriggerEvent()
                    data_events[i] = deepcopy(data_event)
            
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

def data_event_call(data_events):
    try:
        while True:
            scheduler.run()
    except Exception as e:
        print(f"data_event_call: Error {e}")
    finally:
        client_mqtt.loop_stop()
        client_mqtt.disconnect()

def data_event_time(data_events):
    try:
        client_local = mqtt.Client()
        client_local.connect(broker_address, port)
        while True:
            for data_event in data_events:
                client_local.publish(topic_local, f"{int(datetime.datetime.now().timestamp())},{data_event.metric},time;")
                time.sleep(5)
    except Exception as e:
        print(f"data_event_time: Error {e}")
    finally:
        client_local.disconnect()

def run():
    process_1 = multiprocessing.Process(target=data_event_call, args=(data_events, ))
    process_2 = multiprocessing.Process(target=data_event_time, args=(data_events, ))

    process_1.start()
    process_2.start()

    input("Presiona Enter para detener los procesos...\n")
    process_1.terminate()
    process_2.terminate()

    process_1.join()
    process_2.join()

    executor.shutdown()