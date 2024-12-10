import socket
import threading
from QueryProcessor.QueryProcessor import QueryProcessor

def handle_client(client_socket, query_processor):
    try:
        client_socket.send(b"Welcome to PostgreysiaSQL! You can start using the SQL commands.\n")
        while True:
            try:
                query = client_socket.recv(1024).decode("utf-8").strip()
                if query.lower() == "exit":
                    client_socket.send(b"Goodbye!\n")
                    break

                response = query_processor.execute_query(query)
                client_socket.send(f"{response}\n".encode("utf-8"))

            except Exception as e:
                client_socket.send(f"Error: {e}\n".encode("utf-8"))

    except Exception as e:
        print(f"Client connection error: {e}")
    finally:
        client_socket.close()

def start_server(host="127.0.0.1", port=65432):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen()
    print(f"Server started on {host}:{port}. Waiting for clients...")
    
    query_processor = QueryProcessor()
    
    while True:
        client_socket, addr = server.accept()
        print(f"Client connected from {addr}")
        client_handler = threading.Thread(target=handle_client, args=(client_socket, query_processor))
        client_handler.start()

if __name__ == "__main__":
    start_server()
