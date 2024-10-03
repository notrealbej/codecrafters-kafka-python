import socket  # noqa: F401
import struct

def create_message(coRelationID: int, errorCode: int | None = None) -> bytes:
    message = coRelationID.to_bytes(4, byteorder="big")
    if(errorCode != None):
        message += errorCode.to_bytes(2, byteorder="big")

    messageLen = len(message).to_bytes(4, byteorder="big")
    return messageLen + message

def handle_client(client):
    data = client.recv(1024)
    apiVersion = int.from_bytes(data[6:8], byteorder="big")
    coRelationID = int.from_bytes(data[8:12], byteorder="big")
    errorCode = None
    if(apiVersion not in [0, 1, 2, 3, 4]):
        errorCode = 35

    client.sendall(create_message(coRelationID, errorCode))
    client.close()


def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        client, addr = server.accept() 
        handle_client(client)


if __name__ == "__main__":
    main()
