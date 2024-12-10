import datetime
import socket
from pymongo.mongo_client import MongoClient
from urllib.parse import quote_plus

# MongoDB credentials
username = quote_plus('yewoookie')
password = quote_plus('madefor327')

def process_query(selected_query, database):
    collection = database['DB1_virtual']

    if selected_query == "1":
        # Calculate the average moisture for the kitchen fridge in the past 3 hours
        pipeline = [
            {"$match": {
                "payload.board_name": "Raspberry Pi 4 - raspi",
                "time": {
                    "$gte": datetime.datetime.utcnow() - datetime.timedelta(hours=3),
                    "$lte": datetime.datetime.utcnow()
                }
            }},
            {"$group": {
                "_id": None,
                "average_moisture": {"$avg": {"$toDouble": "$payload.Moisture Meter - Moisture meter"}}
            }}
        ]
        try:
            result = collection.aggregate(pipeline)
            avg_moisture = next(result, {}).get("average_moisture")
            if avg_moisture is None:
                return "No data found for average moisture in the kitchen fridge."
            return f"Average moisture in the kitchen fridge over the past 3 hours: {avg_moisture:.2f} RH%"
        except Exception as e:
            return f"Error while calculating average moisture: {str(e)}"

    elif selected_query == "2":
        # Calculate the average water consumption for the dishwasher
        pipeline = [
            {"$match": {"payload.board_name": "board 2 ed0866b9-a546-48d0-b81d-bad98b125f8c"}},
            {"$group": {
                "_id": None,
                "average_water_consumption": {"$avg": {"$toDouble": "$payload.water consumption sensor"}}
            }}
        ]
        try:
            result = collection.aggregate(pipeline)
            avg_consumption = next(result, {}).get("average_water_consumption")
            if avg_consumption is None:
                return "No data found for average water consumption."
            return f"Average water consumption per cycle: {avg_consumption:.2f} Liters"
        except Exception as e:
            return f"Error while calculating average water consumption: {str(e)}"

    elif selected_query == "3":
    # Determine which device consumed more electricity
        pipeline = [
            # Match documents with a valid Ammeter value
            {"$match": {"payload.Ammeter": {"$exists": True}}},
            
            # Convert Ammeter value to double and divide by 1,000 to convert Wh to kWh
            {"$project": {
                "board_name": "$payload.board_name",
                "ammeter_value_kwh": {"$divide": [{"$toDouble": "$payload.Ammeter"}, 1000]}
            }},
            
            # Group by board_name and calculate the total consumption in kWh
            {"$group": {
                "_id": "$board_name",
                "total_consumption": {"$sum": "$ammeter_value_kwh"}
            }},
            
            # Sort by total consumption in descending order
            {"$sort": {"total_consumption": -1}},
            
            # Limit to the top device
            {"$limit": 1}
        ]
        try:
            result = collection.aggregate(pipeline)
            top_device = next(result, {})
            device_name = top_device.get("_id", "Unknown device")
            total_consumption = top_device.get("total_consumption", 0)
            if device_name is None:
                return "No data found for electricity consumption."
            return f"The device that consumed the most electricity is: {device_name} with a total consumption of {total_consumption:.2f} kWh."
        except Exception as e:
            return f"Error while calculating electricity consumption: {str(e)}"




def start_server():
    uri = f"mongodb+srv://{username}:{password}@cluster0.2uyj5.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    db = client["test"]

    try:
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ip_address = input("Enter the IP address to bind the server on: ")
    port_number = input("Enter the port number to bind the server: ")

    try:
        port_number = int(port_number)
        server_socket.bind((ip_address, port_number))
        server_socket.listen(5)
        print(f"Server is listening on {ip_address}:{port_number}")

        while True:
            client_socket, client_address = server_socket.accept()
            print(f"Connection from {client_address} established.")

            while True:
                message = client_socket.recv(1024)
                if not message:
                    break
                query_selection = message.decode()
                if query_selection == 'exit':
                    print(f"Connection from {client_address} closed.")
                    break

                response = process_query(query_selection, db)
                client_socket.send(response.encode())

            client_socket.close()

    except ValueError:
        print("Error: Port number must be an integer.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        server_socket.close()

if __name__ == "__main__":
    start_server()
