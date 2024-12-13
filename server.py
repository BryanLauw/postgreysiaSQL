import socket
import threading
import argparse
from QueryProcessor.QueryProcessor import QueryProcessor

client_states = {}

def handle_client(client_socket, client_id, query_processor):
    try:
        client_socket.send(b"Welcome to PostgreysiaSQL! You can start using SQL commands.\n")
        
        while True:
            try:
                query = client_socket.recv(1024).decode("utf-8").strip()
                
                if query.lower() == "exit":
                    client_socket.send(b"Goodbye!\n")
                    break
                
                list_of_queries = query_processor.parse_query(query)
                for query in list_of_queries:
                    response = query_processor.execute_query(query, client_id)
                client_socket.send(f"{response}\n".encode("utf-8"))
            except Exception as e:
                error_message = f"Error server: {e}"
                client_socket.send(f"{error_message}\n".encode("utf-8"))
    except Exception as e:
        print(f"Client connection error: {e}")
    finally:
        if client_id in client_states:
            del client_states[client_id]
        client_socket.close()

def start_server(database_name, host="127.0.0.1", port=65432):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen()
    print(f"Server started on {host}:{port}. Waiting for clients...")
    
    query_processor = QueryProcessor(database_name)
    client_counter = 0
    
    while True:
        client_socket, addr = server.accept()
        print(f"Client connected from {addr}")
        client_id = client_counter
        client_counter += 1

        # Start a thread to handle the client
        client_handler = threading.Thread(target=handle_client, args=(client_socket, client_id, query_processor))
        client_handler.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the PostgreysiaSQL server.")
    parser.add_argument("database_name", type=str, help="The name of the database to use.")
    args = parser.parse_args()

    start_server(database_name=args.database_name)
