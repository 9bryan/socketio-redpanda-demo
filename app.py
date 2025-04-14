from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
import threading
import os

# Load settings from .env
load_dotenv()

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Kafka config from .env
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
KAFKA_SEND_TOPIC = os.getenv("KAFKA_SEND_TOPIC", "send-topic")
KAFKA_RECEIVE_TOPIC = os.getenv("KAFKA_RECEIVE_TOPIC", "receive-topic")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "flask-consumer-group")

# Kafka producer (sending plain strings)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: v.encode('utf-8'),
    security_protocol=KAFKA_SECURITY_PROTOCOL,
    sasl_mechanism=KAFKA_SASL_MECHANISM,
    sasl_plain_username=KAFKA_USERNAME,
    sasl_plain_password=KAFKA_PASSWORD
)

# Kafka consumer (decoding plain strings)
def consume_kafka():
    consumer = KafkaConsumer(
        KAFKA_RECEIVE_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: m.decode('utf-8'),
        auto_offset_reset='latest',
        group_id=KAFKA_GROUP_ID,
        security_protocol=KAFKA_SECURITY_PROTOCOL,
        sasl_mechanism=KAFKA_SASL_MECHANISM,
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD
    )

    for message in consumer:
        print("Kafka received:", message.value)
        socketio.emit('kafka_message', message.value)

@socketio.on('send_to_kafka')
def handle_send_to_kafka(data):
    print("Sending to Kafka:", data)
    # Assume data is already a string
    producer.send(KAFKA_SEND_TOPIC, data)

@app.route("/")
def index():
    return render_template("index.html")

if __name__ == '__main__':
    threading.Thread(target=consume_kafka, daemon=True).start()
    socketio.run(app, host='0.0.0.0', port=5000)

