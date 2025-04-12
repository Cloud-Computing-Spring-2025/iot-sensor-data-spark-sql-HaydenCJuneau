import csv
import random
from faker import Faker

fake = Faker()

LOCATIONS = ["BuildingA_Floor1", "BuildingA_Floor2", "BuildingB_Floor1", "BuildingB_Floor2"]
SENSOR_TYPES = ["TypeA", "TypeB", "TypeC"]

def generate_sensor_data(num_records=1000, output_file="sensor_data.csv"):
    """
    Generates a CSV with fields:
    sensor_id, timestamp, temperature, humidity, location, sensor_type
    """
    fieldnames = ["sensor_id", "timestamp", "temperature", "humidity", "location", "sensor_type"]
    
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for _ in range(num_records):
            sensor_id = random.randint(1000, 1100)  # e.g., sensor IDs between 1000 and 1100
            timestamp_str = fake.date_time_between(start_date="-5d", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
            temperature_val = round(random.uniform(15.0, 35.0), 2)  # range 15 to 35
            humidity_val = round(random.uniform(30.0, 80.0), 2)     # range 30% to 80%
            location_val = random.choice(LOCATIONS)
            sensor_type_val = random.choice(SENSOR_TYPES)
            
            writer.writerow({
                "sensor_id": sensor_id,
                "timestamp": timestamp_str,
                "temperature": temperature_val,
                "humidity": humidity_val,
                "location": location_val,
                "sensor_type": sensor_type_val
            })

if __name__ == "__main__":
    generate_sensor_data(num_records=1000, output_file="sensor_data.csv")
    print("sensor_data.csv generated.")
