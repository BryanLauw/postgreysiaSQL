import socket

def start_client(host="127.0.0.1", port=65432):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((host, port))
    print("connect")

    while True:
        try:
            server_message = client.recv(1024).decode("utf-8")
            print(server_message.strip())

            query = input("> ").strip()
            client.send(query.encode("utf-8"))

            if query.lower() == "exit":
                print("Exit")
                break
            
            response = client.recv(1024).decode("utf-8")
            print(response.strip())
        except Exception as e:
            print(f"Error: {e}")
            break

    client.close()

if __name__ == "__main__":
    start_client()
