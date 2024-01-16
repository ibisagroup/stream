import datetime
import random
import paho.mqtt.client as mqtt
import time

# Configura los detalles del servidor MQTT
broker_address = "127.0.0.1"  # Cambia esto al servidor MQTT que desees usar
port = 1883
topic = "devteam/888888/stream"  # Cambia esto al tópico que desees usar

# Callback que se ejecuta cuando se conecta al servidor MQTT
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conexión exitosa al servidor MQTT")
    else:
        print("Error de conexión al servidor MQTT, código:", rc)

# Inicializa el cliente MQTT
client = mqtt.Client()  # Cambia "mi_cliente" al nombre de tu cliente
client.on_connect = on_connect

# Conéctate al servidor MQTT
client.connect(broker_address, port)

# Publica señales en el tópico
while True:
    # Genera una señal aleatoria (puedes reemplazar esto con tus propios datos o lógica)
    signal = random.randint(0, 10)
    #signal = 1
    
    # Publica la señal en el tópico MQTT
    client.publish(topic, f"{int(datetime.datetime.now().timestamp())},devteam.prueba_0,{signal};")
    print(f"Señal aleatoria publicada en el tópico '{topic}': {signal}")

    # Espera un intervalo de tiempo antes de publicar la siguiente señal (en segundos)
    time.sleep(5)  # Cambia el intervalo según tus necesidades

# Desconéctate del servidor MQTT
client.disconnect()