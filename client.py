import socket

def validate_input():
    valid_inputs = ["1", "2", "3", "exit"]
    while True:
        user_input = input("Enter 1, 2, or 3 (or 'exit' to quit): ")
        if user_input in valid_inputs:
            if user_input == "exit":
                print("Program successfully exited. Thank you!")
                return "exit"
            return user_input
        else:
            print("Invalid input. Please try again (1, 2, or 3).")

def query_function():
    print("Select one of the following queries (type 'exit' to quit):")
    print("1. What is the average moisture inside my kitchen fridge in the past three hours?")
    print("2. What is the average water consumption per cycle in my smart dishwasher?")
    print("3. Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)?")
    return validate_input()

def start_client():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ip_address = input("Enter the server IP address: ")
    port_number = input("Enter the server port number: ")
    
    try:
        port_number = int(port_number)
        client_socket.connect((ip_address, port_number))
        print(f"Connected to server at {ip_address}:{port_number}")

        while True:
            message = query_function()
            if message == 'exit':
                client_socket.send(message.encode())
                break
            
            client_socket.send(message.encode())
            server_reply = client_socket.recv(1024)
            print(f"Server reply: {server_reply.decode()}")
    
    except ValueError:
        print("Error: Port number must be an integer.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client_socket.close()

if __name__ == "__main__":
    start_client()
