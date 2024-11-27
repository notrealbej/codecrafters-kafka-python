import socket
import threading
from enum import Enum, unique


def fetch_message(correlation_id: int, api_key: int, api_version: int):
    min_version, max_version = 0, 16
    throttle_time_ms = 0
    tag_buffer = b"\x00"
    session_id = 0
    responses = []

    error_code = 0
    if max_version < api_version or api_version < min_version: 
        error_code = 35

    message = correlation_id.to_bytes(4, byteorder="big") + throttle_time_ms.to_bytes(4, byteorder="big", signed=True)
    message += (error_code).to_bytes(2, byteorder="big", signed=True) + (session_id).to_bytes(4, byteorder="big", signed=True)
    message += (len(responses) + 1).to_bytes(1, byteorder="big"gi) + tag_buffer

    return message

def apiversion_message(correlation_id: int, api_key: int, api_version: int):
    min_version, max_version = 0, 4
    throttle_time_ms = 0
    tag_buffer = b"\x00"

    error_code = 0
    if max_version < api_version or api_version < min_version: 
        error_code = 35

    message = correlation_id.to_bytes(4, byteorder="big")
    message += error_code.value.to_bytes(2, byteorder="big") + int(3).to_bytes(1, byteorder="big") #3 indicates 2 api keys
    message += api_key.to_bytes(2, byteorder="big") + min_version.to_bytes(2, byteorder="big")
    message += max_version.to_bytes(2, byteorder="big") + tag_buffer
    message += (1).to_bytes(2, byteorder="big") + min_version.to_bytes(2, byteorder="big")
    message += (16).to_bytes(2, byteorder="big") + tag_buffer
    message += throttle_time_ms.to_bytes(4, byteorder="big") + tag_buffer

    return message



def create_message(correlation_id: int, api_key: int, api_version: int) -> bytes:
    message = ""
    if api_key == 1:
        message = fetch_message(correlation_id, api_key, api_version)
    elif api_key == 18:
        message = apiversion_message(correlation_id, api_key, api_version)

    message_len = len(message).to_bytes(4, byteorder="big")
    return message_len + message


def parse_request(data: bytes) -> dict[str, int]:
    return {
        "length": int.from_bytes(data[0:4], byteorder="big"),
        "api_key": int.from_bytes(data[4:6], byteorder="big"),
        "api_version": int.from_bytes(data[6:8], byteorder="big"),
        "correlation_id": int.from_bytes(data[8:12], byteorder="big"),
    }

def handler(client):
    while True:
        request = client.recv(2048)
        if not request:
            break

        request_data = parse_request(request)

        message = create_message(
            request_data["correlation_id"], request_data["api_key"], request_data["api_version"]
        )
        client.sendall(message)

    client.close()


def main() -> None:
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        client, _ = server.accept()
        threading.Thread(target=handler, args=(client,), daemon=True).start()


if __name__ == "__main__":
    main()
