import random
import re
import json
import time
from datetime import datetime
from kafka import KafkaProducer
import os

# Kafka Config
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'weather-api-data')
KAFKA_BROKER_1 = os.environ.get('KAFKA_BROKER_1', 'localhost:9092')
# KAFKA_BROKER_2 = os.environ.get('KAFKA_BROKER_2', 'localhost:9093')
# KAFKA_BROKER_3 = os.environ.get('KAFKA_BROKER_3', 'localhost:9094')

# KAFKA_BOOTSTRAP_SERVERS = [KAFKA_BROKER_1, KAFKA_BROKER_2, KAFKA_BROKER_3]

KAFKA_BOOTSTRAP_SERVERS = [KAFKA_BROKER_1]

# Bandara
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

# Ambil nama kota dari nama bandara
def extract_city_name(airport):
    return re.sub(r"(International\s)?Airport.*|Air Base.*|\s+\(.*?\)", "", airport).strip()

cities = [extract_city_name(name) for name in airport_names]

# Kondisi cuaca
conditions = ["Clear", "Cloudy", "Rain", "Thunderstorm", "Fog", "Snow"]

# Simpan data cuaca sebelumnya untuk tiap kota
previous_weather = {
    city: {
        "temperature": round(random.uniform(-10.0, 30.0), 1),
        "humidity": random.randint(40, 80),
        "wind_speed": round(random.uniform(1.0, 10.0), 1),
        "weather_condition": random.choice(conditions)
    }
    for city in cities
}

# Fungsi pembaruan nilai secara bertahap
def gradual_update(value, min_val, max_val, delta=1.0):
    change = random.uniform(-delta, delta)
    new_val = value + change
    return max(min(round(new_val, 1), max_val), min_val)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Fungsi utama
def generate_and_send():
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    for city in cities:
        prev = previous_weather[city]

        # Perbarui secara bertahap
        new_temp = gradual_update(prev["temperature"], -30.0, 40.0, delta=0.3)
        new_humidity = int(gradual_update(prev["humidity"], 20, 100, delta=2.5))
        new_wind_speed = gradual_update(prev["wind_speed"], 0.0, 30.0, delta=0.5)

        # 10% kemungkinan berubah kondisi cuaca
        if random.random() < 0.1:
            prev["weather_condition"] = random.choice(conditions)

        # Simpan pembaruan
        previous_weather[city].update({
            "temperature": new_temp,
            "humidity": new_humidity,
            "wind_speed": new_wind_speed,
        })

        # Format data seperti yang diminta
        weather_data = {
            "timestamp": timestamp,
            "location": city,
            "weather_condition": prev["weather_condition"],
            "details": {
                "temperature": new_temp,
                "humidity": new_humidity,
                "wind_speed": new_wind_speed
            }
        }

        producer.send(KAFKA_TOPIC, value=weather_data)
        print(f"Sent to Kafka: {weather_data}")

    producer.flush()

# Main loop
if __name__ == "__main__":
    try:
        while True:
            print(f"Generating and sending weather data at {datetime.utcnow()}")
            generate_and_send()
            time.sleep(1)  # setiap detik
    except KeyboardInterrupt:
        print("Stopped.")