import socket
import threading
from QueryProcessor.QueryProcessor import QueryProcessor

client_states = {}

def handle_client(client_socket,client_id, query_processor):
    try:
        client_states[client_id] = {"on_begin": False, "transactionId": None}

        client_socket.send(b"Welcome to PostgreysiaSQL! You can start using the SQL commands.\n")
        while True:
            try:
                query = client_socket.recv(1024).decode("utf-8").strip()
                
                if query.lower() == "exit":
                    client_socket.send(b"Goodbye!\n")
                    break

                print(f"Client {client_id} state before query: {client_states[client_id]}")
                response = query_processor.execute_query(query,client_states[client_id])
                client_socket.send(f"{response}\n".encode("utf-8"))
            except Exception as e:
                error_message = f"Error server: {e}"
                client_socket.send(f"{error_message}\n".encode("utf-8"))
    except Exception as e:
        print(f"Client connection error: {e}")
    finally:
        del client_states[client_id]
        client_socket.close()

def start_server(host="127.0.0.1", port=65432):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen()
    print(f"Server started on {host}:{port}. Waiting for clients...")
    
    query_processor = QueryProcessor()
    client_counter = 0
    
    while True:
        client_socket, addr = server.accept()
        print(f"Client connected from {addr}")
        client_id = client_counter
        client_counter += 1
        client_handler = threading.Thread(target=handle_client, args=(client_socket, client_id, query_processor))
        client_handler.start()

if __name__ == "__main__":
    start_server()
