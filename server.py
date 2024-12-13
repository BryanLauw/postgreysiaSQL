import socket
import threading
import argparse
from QueryProcessor.QueryProcessor import QueryProcessor
from client_class import Client
import signal

class Server:
    def __init__(self, database_name, host="127.0.0.1", port=65432):
        self.database_name = database_name
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_counter = 0  
        self.clients = {} 
        self.query_processor = QueryProcessor(database_name, self.clients) 

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        print(f"Server started on {self.host}:{self.port}. Waiting for clients...")

        while True:
            client_socket, addr = self.server_socket.accept()
            print(f"Client connected from {addr}")
            client_id = self.client_counter
            self.client_counter += 1

            client = Client(client_id, client_socket)
            self.clients[client_id] = client
       
            client_thread = threading.Thread(target=self.handle_client, args=(client,))
            client_thread.start()

    def handle_client(self, client: Client):
        try:
            client.send("Welcome to PostgreysiaSQL! You can start using SQL commands.")

            while True:
                query = client.receive()

                if query.lower() == "exit":
                    client.send("Goodbye!")
                    break

                if not query:
                    client.send("Empty query, please re-enter.")
                    continue

                if query.lower() == "interrupt":
                    print(f"Client {client.client_id} interrupted the connection.")
                    self.query_processor.signal_handler(client.client_id, signal.SIGINT, None)  
                    break
              
                list_of_queries = self.query_processor.parse_query(query)
                for q in list_of_queries:
                    response = self.query_processor.execute_query(q, client)
                client.send(response)
        except Exception as e:
            print(f"Error handling client {client.client_id}: {e}")
        finally:
            if client.client_id in self.clients:
                del self.clients[client.client_id]
            client.socket.close()

    def signal_handler(self, signum, frame):
        print(f"Signal {signum} received. Cleaning up...")
        for client_id in self.clients.items():
            self.query_processor.signal_handler(client_id, signum, frame)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the PostgreysiaSQL server.")
    parser.add_argument("database_name", type=str, help="The name of the database to use.")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="The host IP address for the server.")
    parser.add_argument("--port", type=int, default=65432, help="The port number for the server.")
    args = parser.parse_args()

    server = Server(database_name=args.database_name, host=args.host, port=args.port)
    server.start()