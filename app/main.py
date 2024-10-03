import socket  # noqa: F401
import struct

def create_message(id):
    id_bytes = id.to_bytes(4, byteorder="big")
    return len(id_bytes).to_bytes(4, byteorder="big") + id_bytes

def handle_client(client):
    data = client.recv(1024)
    apiVersion = int.from_bytes(data[6:8], byteorder="big")
    coRelationID = int.from_bytes(data[8:12], byteorder="big")
    client.sendall(create_message(coRelationID))
    if(apiVersion not in [0, 1, 2, 3, 4]):
        client.sendall(create_message(35))
    client.close()


def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        client, addr = server.accept() 
        handle_client(client)


if __name__ == "__main__":
    main()
