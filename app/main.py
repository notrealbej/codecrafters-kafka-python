import socket  # noqa: F401
import struct

def create_message(id):
    id_bytes = id.to_bytes(4, byteorder="big")
    return len(id_bytes).to_bytes(4, byteorder="big") + id_bytes

def handle_client(client):
    data = client.recv(1024)
    client.sendall(create_message(struct.unpack("!i", data)[1]))
    client.close()


def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        client, addr = server.accept() 
        handle_client(client)


if __name__ == "__main__":
    main()
