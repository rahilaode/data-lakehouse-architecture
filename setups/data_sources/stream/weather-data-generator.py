import random
import re
import json
import time
from datetime import datetime
from kafka import KafkaProducer
import os

# Import environment variables
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC')
KAFKA_BROKER_1 = os.environ.get('KAFKA_BROKER_1')
KAFKA_BROKER_2 = os.environ.get('KAFKA_BROKER_2')
KAFKA_BROKER_3 = os.environ.get('KAFKA_BROKER_3')

# Kafka
KAFKA_TOPIC = 'weather-api-data'
KAFKA_BOOTSTRAP_SERVERS = [KAFKA_BROKER_1, KAFKA_BROKER_2, KAFKA_BROKER_3]  

# Airport list
airport_names = [
    "Yakutsk Airport", "Mirny Airport", "Khabarovsk-Novy Airport", "Yelizovo Airport",
    "Yuzhno-Sakhalinsk Airport", "Vladivostok International Airport", "Pulkovo Airport",
    "Khrabrovo Airport", "Kemerovo Airport", "Chelyabinsk Balandino Airport",
    "Magnitogorsk International Airport", "Bolshoye Savino Airport", "Surgut Airport",
    "Bryansk Airport", "Mineralnyye Vody Airport", "Stavropol Shpakovskoye Airport",
    "Astrakhan Airport", "Nizhnevartovsk Airport", "Koltsovo Airport",
    "Sheremetyevo International Airport", "Voronezh International Airport",
    "Vnukovo International Airport", "Syktyvkar Airport", "Kurumoch International Airport",
    "Domodedovo International Airport", "Roshchino International Airport",
    "Nizhny Novgorod Strigino International Airport", "Bogashevo Airport", "Ust-Ilimsk Airport",
    "Norilsk-Alykel Airport", "Talagi Airport", "Saratov Central Airport", "Novy Urengoy Airport",
    "Noyabrsk Airport", "Ukhta Airport", "Usinsk Airport", "Naryan Mar Airport", "Pskov Airport",
    "Kogalym International Airport", "Yemelyanovo Airport", "Uray Airport", "Ivanovo South Airport",
    "Polyarny Airport", "Komsomolsk-on-Amur Airport", "Ugolny Airport", "Petrozavodsk Airport",
    "Kyzyl Airport", "Spichenkovo Airport", "Khankala Air Base", "Nalchik Airport", "Beslan Airport",
    "Elista Airport", "Salekhard Airport", "Khanty Mansiysk Airport", "Nyagan Airport",
    "Sovetskiy Airport", "Izhevsk Airport", "Pobedilovo Airport", "Nadym Airport",
    "Nefteyugansk Airport", "Kurgan Airport", "Belgorod International Airport", "Kursk East Airport",
    "Lipetsk Airport", "Vorkuta Airport", "Bugulma Airport", "Yoshkar-Ola Airport",
    "Cheboksary Airport", "Ulyanovsk East Airport", "Orsk Airport", "Penza Airport",
    "Saransk Airport", "Donskoye Airport", "Ust-Kut Airport", "Gelendzhik Airport",
    "Tunoshna Airport", "Begishevo Airport", "Ulyanovsk Baratayevka Airport",
    "Strezhevoy Airport", "Beloyarskiy Airport", "Grabtsevo Airport", "Gorno-Altaysk Airport",
    "Krasnodar Pashkovsky International Airport", "Uytash Airport", "Kazan International Airport",
    "Orenburg Central Airport", "Ufa International Airport", "Tolmachevo Airport",
    "Cherepovets Airport", "Omsk Central Airport", "Rostov-on-Don Airport",
    "Sochi International Airport", "Volgograd International Airport", "Ignatyevo Airport",
    "Sokol Airport", "Chita-Kadala Airport", "Bratsk Airport", "Irkutsk Airport",
    "Ulan-Ude Airport (Mukhino)", "Murmansk Airport", "Abakan Airport", "Barnaul Airport",
    "Anapa Vityazevo Airport", "Chulman Airport"
]

# Kondisi cuaca
conditions = ["Clear", "Cloudy", "Rain", "Thunderstorm", "Fog", "Snow"]

# Ekstrak nama kota
def extract_city_name(airport):
    return re.sub(r"(International\s)?Airport.*|Air Base.*|\s+\(.*?\)", "", airport).strip()

cities = [extract_city_name(name) for name in airport_names]

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Fungsi utama
def generate_and_send():
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    for city in cities:
        weather_data = {
            "timestamp": timestamp,
            "location": city,
            "weather_condition": random.choice(conditions),
            "details": {
                "temperature": round(random.uniform(-30.0, 40.0), 1),
                "humidity": random.randint(20, 100),
                "wind_speed": round(random.uniform(0.0, 30.0), 1),
            },
        }
        producer.send(KAFKA_TOPIC, value=weather_data)
        print(f"Sent to Kafka: {weather_data}")
    producer.flush()

# Loop terus setiap men10 detik
if __name__ == "__main__":
    try:
        while True:
            print(f"Generating and sending weather data at {datetime.utcnow()}")
            generate_and_send()
            time.sleep(10)  # Tunggu 10 detik
    except KeyboardInterrupt:
        print("Stopped.")