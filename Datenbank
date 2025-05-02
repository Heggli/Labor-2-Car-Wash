# Labor-2-Car-Wash
import paho.mqtt.client as mqtt
import socket
import threading

# MQTT broker details
broker_address = "localhost"
broker_port = 1883
topics = [ ("SFB/Aktor/Q1", 0),("SFB/Aktor/Q2", 0),("SFB/Aktor/Q3", 0),("SFB/Aktor/Q4", 0),("SFB/Aktor/Q5", 0),("SFB/Aktor/Q6", 0),
            ("SFB/Aktor/P1", 0),("SFB/Aktor/P2", 0),("SFB/Aktor/P3", 0),("SFB/Aktor/P4", 0),
            ("SFB/Aktor/Y1", 0),("SFB/Aktor/Y2", 0),("SFB/Aktor/Y3", 0),("SFB/Aktor/Y4", 0),("SFB/Aktor/Y5", 0),    
        ]  

# UDP-Verbindung vorbereiten
UDP_IP = "127.0.0.1"
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP-Socket erstellen

# Port-Liste 
Topic_Port = {
"SFB/Aktor/Q1":1705,"SFB/Aktor/Q2":1706,"SFB/Aktor/Q3":1707,"SFB/Aktor/Q4":1708,"SFB/Aktor/Q5":1709,"SFB/Aktor/Q6":1710,
"SFB/Aktor/P1":1711,"SFB/Aktor/P2":1712,"SFB/Aktor/P3":1713,"SFB/Aktor/P4":1714,
"SFB/Aktor/Y1":1700,"SFB/Aktor/Y2":1701,"SFB/Aktor/Y3":1702,"SFB/Aktor/Y4":1703,"SFB/Aktor/Y5":1704,
}



# Port-zu-Topic Mapping
Port_Topic = {
    1600: "SFB/Sensor/S0", 1601: "SFB/Sensor/S1", 1602: "SFB/Sensor/S2", 1603: "SFB/Endschalter/B1",
    1604: "SFB/Endschalter/B2", 1605: "SFB/Endschalter/B3", 1606: "SFB/Endschalter/B4", 1607: "SFB/Endschalter/B5",
    1608: "SFB/Endschalter/B6", 1609: "SFB/Endschalter/B7", 1610: "SFB/Endschalter/B8", 
    
    }


# Callback-Funktion für empfangene MQTT-Nachrichten
def on_message(client, userdata, message):
    topic = message.topic
    payload = message.payload.decode()  # Nachricht in String umwandeln

    # Überprüfen, ob das Topic in port_liste existiertx
    if topic in Topic_Port:
        port = Topic_Port[topic]
        print(f"Empfangene Nachricht von {topic} (Port {port}): {payload}")

        # Nachricht per UDP weiterleiten
        udp_message = str(payload).encode('utf-8')  # 🔹 Fix: Payload korrekt in String umwandeln
        sock.sendto(udp_message, (UDP_IP, port))
    else:
        print(f"Unbekanntes Topic: {topic}")


# Liste der Ports, die UDP-Nachrichten empfangen sollen
ports = list(Port_Topic.keys())


# Callback für MQTT-Verbindung
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Erfolgreich mit MQTT-Broker verbunden!")
    else:
        print(f"⚠ Fehler bei der MQTT-Verbindung! Rückgabecode: {rc}")

    # Abonniere alle relevanten Topics
    for topic in set(Port_Topic.values()):
        client.subscribe(topic)
        print(f"📡 Abonniert: {topic}")


# Funktion für den Empfang von UDP-Nachrichten auf einem bestimmten Port
def Empfang_socket_Sendet_MQTT(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, port))
    print(f"🎧 UDP-Listener gestartet: Port {port} gebunden.")

    while True:
        try:
            data, addr = sock.recvfrom(1024)
            msg = data.decode().strip()  # Entfernt Leerzeichen/Zeilenumbrüche
            print(f"🔹 Port {port} -> Nachricht empfangen: {msg} von {addr}")

            # MQTT-Nachricht senden
            topic = Port_Topic.get(port)
            if topic:
                client.publish(topic, msg)
                print(f"📤 MQTT gesendet: '{msg}' -> Thema: '{topic}'")
            else:
                print(f"⚠ Kein MQTT-Topic für Port {port} gefunden!")

        except Exception as e:
            print(f"❌ Fehler bei Port {port}: {e}")

# Starte Threads für UDP-Listener
def start_udp_threads():
    for port in ports:
        thread = threading.Thread(target=Empfang_socket_Sendet_MQTT, args=(port,))
        thread.daemon = True  # Beendet sich automatisch mit dem Hauptprogramm
        thread.start()



# MQTT-Client erstellen
client = mqtt.Client()
client.on_message = on_message  # Callback-Funktion registrieren
client.on_connect = on_connect
# Verbindung zum Broker herstellen
client.connect(broker_address, broker_port)

# Topics abonnieren
client.subscribe(topics)

  # Starte UDP-Listener in separaten Threads
start_udp_threads()

# MQTT-Loop starten
client.loop_forever()
