import socket
import signal
import sys

def recv_all(client):
    buffer = ""
    while True:
        data = client.recv(1024).decode("utf-8")
        if not data:  
            break
        buffer += data
        if buffer.endswith("\n"):  
            break
    return buffer.strip()  

def signal_handler(signal, frame, client):
    print("\nCtrl+C detected. Exiting gracefully...")
    client.send("interrupt\n".encode("utf-8"))  
    client.close()
    sys.exit(0)

def start_client(host="127.0.0.1", port=65432):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((host, port))

    signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, client))

    try:
        print(recv_all(client))

        while True:
            query = input("> ").strip()

            if query.lower() == "exit":
                client.send(query.encode("utf-8"))
                print("Goodbye!")
                break

            if not query.endswith(";"):
                print("Invalid query syntax. Queries must end with a semicolon.")
                continue

            if not query:
                print("Empty query, please re-enter.")
                continue

            client.send(f"{query}\n".encode("utf-8"))

            response = recv_all(client)
            print(response)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    start_client()