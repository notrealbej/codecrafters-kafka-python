import socket  # noqa: F401
import struct


def create_message(coRelationID: int, errorCode: int, apiKey: int) -> bytes:
    min_version, max_version = 0, 4
    throttle_time_ms = 0
    tag_buffer = b"\x00"

    message = coRelationID.to_bytes(4, byteorder="big")
    message += errorCode.to_bytes(2, byteorder="big") + int(2).to_bytes(1, byteorder="big") + apiKey.to_bytes(2, byteorder="big") + min_version.to_bytes(2, byteorder="big")
    message += max_version.to_bytes(2, byteorder="big") + tag_buffer + throttle_time_ms.to_bytes(4, byteorder="big") + tag_buffer

    messageLen = len(message).to_bytes(4, byteorder="big")
    return messageLen + message

def parse_request(data: bytes) -> dict[str, int | str]:
    return {
        "length": int.from_bytes(data[0:4]),
        "api_key": int.from_bytes(data[4:6]),
        "api_version": int.from_bytes(data[6:8]),
        "correlation_id": int.from_bytes(data[8:12]),
    }


def main() -> None:
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        client, _ = server.accept()
        request = client.recv(1024)
        request_data = parse_request(request)

        if 0 <= request_data["api_version"] <= 4:
            message = create_message(request_data["correlation_id"], 0, request_data["api_key"])
        else:
            message = create_message(
                request_data["correlation_id"], 35, request_data["api_key"]
            ) 

        client.sendall(message)
    client.close()


if __name__ == "__main__":
    main()
