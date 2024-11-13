import socket
import threading
from enum import Enum, unique


@unique
class ErrorCode(Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35


def create_message(correlation_id: int, error_code: ErrorCode, api_key: int) -> bytes:
    min_version, max_version = 0, 4
    throttle_time_ms = 0
    tag_buffer = b"\x00"

    message = correlation_id.to_bytes(4, byteorder="big")
    message += error_code.value.to_bytes(2, byteorder="big") + int(3).to_bytes(1, byteorder="big")
    message += api_key.to_bytes(2, byteorder="big") + min_version.to_bytes(2, byteorder="big")
    message += max_version.to_bytes(2, byteorder="big") + tag_buffer
    message += throttle_time_ms.to_bytes(4, byteorder="big") + tag_buffer


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
        error_code = (
            ErrorCode.NONE
            if 0 <= request_data["api_version"] <= 4
            else ErrorCode.UNSUPPORTED_VERSION
        )

        message = create_message(
            request_data["correlation_id"], error_code, request_data["api_key"]
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
