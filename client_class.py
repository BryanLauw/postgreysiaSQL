class Client:
    def __init__(self, client_id, socket):
        self.client_id = client_id
        self.socket = socket
        self.state = {"transactionId": None, "on_begin": False}  

    def send(self, message: str):
        self.socket.sendall(f"{message}\n".encode("utf-8"))

    def receive(self) -> str:
        buffer = ""
        while True:
            data = self.socket.recv(1024).decode("utf-8")
            if not data:
                break
            buffer += data
            if buffer.endswith("\n"):
                break
        return buffer.strip()
