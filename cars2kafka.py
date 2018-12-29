import json
import mysql.connector
from mysql.connector import errorcode
from kafka_producer import KafkaProducer
import sys
from random import randint, choice, choices, random
from string import ascii_uppercase
from faker import Faker
import unidecode
from time import sleep


def random_with_N_digits(n):
    range_start = 10 ** (n - 1)
    range_end = (10 ** n) - 1
    return randint(range_start, range_end)

def generate_fake_phone(n):
    return choice(['666','686','609','650','600','625'])+str(random_with_N_digits(n))


def generate_fake_customer(result,fake):
    result['customer'] = {}

    name=fake.name()
    result['customer']['email'] = name.lower().replace(' ','') + str(randint(0, 100)) + '@mail.com'
    result['customer']['email'] = unidecode.unidecode(result['customer']['email'])
    result['customer']['full_name'] = name
    result['customer']['phone_number'] = generate_fake_phone(6)
    return result

def generate_fake_vehicle(result,fake):
    result['vehicle'] = {}
    result['vehicle']['gps_position'] = {}
    result['vehicle']['gps_position']['location'] = {}
    result['vehicle']['gps_position']['location']['lat'] = round(40.41 + random() / 100,6)
    result['vehicle']['gps_position']['location']['lng'] = round(-3.70 + random() / 100,6)
    result['vehicle']['gps_position']['location']['short_display_address'] = 'Calle '+fake.address().replace('\n',',')
    vehicle_id=str(result['vehicle_id'])
    result['vehicle']['license_plate_number'] = str(sum([int(x) for x in vehicle_id]))+\
                                                vehicle_id+"".join(choices(ascii_uppercase, k=3))
    result['vehicle']['fuel_percentage'] = round(random()*100,1)
    result['vehicle']['remaining_range_in_meters'] = int(result['vehicle']['fuel_percentage']*2590)
    result['vehicle']['vin_number'] = 'VF1AGVYF058686'+vehicle_id
    return result

def generate_fake_data():
    fake = Faker('es_ES')
    result = {}
    result['status'] = choice(['BOOKED_DRIVE', 'BOOKED_PARKED', 'RESERVED'])
    result=generate_fake_customer(result,fake)
    result['customer_id'] =random_with_N_digits(5)
    result['event_id'] =str(random_with_N_digits(7))
    result['id'] =random_with_N_digits(7)
    result['vehicle_id'] = randint(0,500)
    result=generate_fake_vehicle(result,fake)
    return result


with open(sys.argv[1]) as config_file:
    config = json.load(config_file)


try:
    conn = mysql.connector.connect(**config['mysql_config'])
    print("Connection established")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with the user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cursor = conn.cursor(buffered=True)
    while True:
        result=generate_fake_data()
        rStatus = result['status']
        customerEmail = result['customer']['email']
        customerFullName = result['customer']['full_name']
        customerPhone = result['customer']['phone_number']
        customerId = result['customer_id']
        eventId = result['event_id']
        rid = result['id']
        vehicleLat = result['vehicle']['gps_position']['location']['lat']
        vehicleLng = result['vehicle']['gps_position']['location']['lng']
        vehicleAddress = result['vehicle']['gps_position']['location']['short_display_address']
        vehiclePlate = result['vehicle']['license_plate_number']
        vehicleFuelPercent = result['vehicle']['fuel_percentage']
        vehicleRemainingKm = result['vehicle']['remaining_range_in_meters'] / 1000
        vehicleVin = result['vehicle']['vin_number']
        vehicleId = result['vehicle_id']
        # Read data
        select_stmt = "SELECT * FROM zity_users WHERE USER_ID = %(uid)s"
        cursor.execute(select_stmt, {'uid': customerId})
        # if cursor.rowcount == 0:
        # Insert data into table
        # cursor.execute("INSERT INTO zity_users (USER_ID, NAME, PHONE, EMAIL) VALUES (%s, %s, %s, %s);", (customerId, customerFullName, customerPhone, customerEmail))
        # Read data
        select_stmt = "SELECT * FROM zity_cars WHERE CAR_ID = %(uid)s"
        cursor.execute(select_stmt, {'uid': vehicleId})
        # if cursor.rowcount == 0:
        # Insert data into table
        #    cursor.execute("INSERT INTO zity_cars (CAR_ID, LICENSE_PLATE, VIN) VALUES (%s, %s, %s);", (vehicleId, vehiclePlate, vehicleVin))
        # Read data
        select_stmt = "SELECT * FROM zity_cars_users WHERE EVENT_ID = %(uid)s"
        cursor.execute(select_stmt, {'uid': eventId})
        if cursor.rowcount == 0:
            # cursor.execute("INSERT INTO zity_cars_users (USER_ID, CAR_ID, STATUS, EVENT_ID, LATITUDE, LONGITUDE) VALUES (%s, %s, %s, %s, %s, %s);", (customerId, vehicleId, rStatus, eventId, vehicleLat, vehicleLng))
            message = {}
            message['user_id'] = customerId
            message['vehicle_id'] = vehicleId
            message['book_status'] = rStatus
            message['event_id'] = eventId
            message['vehicle_lat'] = vehicleLat
            message['vehicle_lng'] = vehicleLng
            message['vehicle_fuel_percent'] = vehicleFuelPercent
            message['vehicle_remaining_km'] = vehicleRemainingKm
            message['book_address'] = vehicleAddress
            json_msg = json.dumps(message)
            #print(message)
            try:
                p = KafkaProducer('user.json')
                p.send(value=message)
            except Exception as e:
                print(e)
    sleep(10)

    # Cleanup
    conn.commit()
    cursor.close()
    conn.close()
    print("Done.")

