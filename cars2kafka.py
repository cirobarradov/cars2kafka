import requests
import json
import mysql.connector
from mysql.connector import errorcode
from kafka_producer import KafkaProducer
import sys
import time

with open(sys.argv[1]) as config_file:
    config = json.load(config_file)

try:
    # get configuration
    #with open('config.json', 'r') as f:
    #    config = json.load(f)
    conn = mysql.connector.connect(**config['mysql_config'])
    print("Connection established")
    try:
        s = requests.Session()

        r = s.post(config['post']['url'],
                   data=config['post']['data'])
        csrfCookie = r.cookies[config['cookie']]
        sessionCookie = config['sessionCookie']
        cfuidCookie = config['cfuidCookie']
        cookieHeader = '__cfduid=' + cfuidCookie + '; ' + 'csrftoken=' + csrfCookie + '; ' + 'sessionid=' + sessionCookie
        headers = {'cookie': cookieHeader, 'accept-encoding': 'gzip, deflate, br',
                   'accept-language': 'es-ES,es;q=0.9,en;q=0.8',
                   'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',
                   'accept': 'application/json', 'referer': config['header']['referer'],
                   'authority': config['header']['authority'], 'x-requested-with': 'XMLHttpRequest'}
        r = s.get(config['url_get'],
            headers=headers)
        response = json.loads(r.text)
        if 'results' not in response:
            print("no results in request:",response)
            sys.exit(0)
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting to "+config['header']['authority']+":", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error in "+config['header']['authority']+":", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with the user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    while True :
        cursor = conn.cursor(buffered=True)
        for result in response['results']:
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
            if cursor.rowcount == 0:
            # Insert data into table
                cursor.execute("INSERT INTO zity_users (USER_ID, NAME, PHONE, EMAIL) VALUES (%s, %s, %s, %s);", (customerId, customerFullName, customerPhone, customerEmail))
            # Read data
            select_stmt = "SELECT * FROM zity_cars WHERE CAR_ID = %(uid)s"
            cursor.execute(select_stmt, {'uid': vehicleId})
            if cursor.rowcount == 0:
            # Insert data into table
                cursor.execute("INSERT INTO zity_cars (CAR_ID, LICENSE_PLATE, VIN) VALUES (%s, %s, %s);", (vehicleId, vehiclePlate, vehicleVin))
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

        # Cleanup
        conn.commit()
        cursor.close()
        time.sleep(60)
    conn.close()
    print("Done.")

