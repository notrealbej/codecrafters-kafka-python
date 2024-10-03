import socket  # noqa: F401
import struct

def create_message(coRelationID: int, errorCode: int, apiKey: int) -> bytes:
    message = coRelationID.to_bytes(4, byteorder="big")
    message += errorCode.to_bytes(2, byteorder="big")
    message += apiKey.to_bytes(2, byteorder="big")
    messageLen = len(message).to_bytes(4, byteorder="big")
    return messageLen + message

def parse_request(request: bytes) -> dict[str, int | str]:
    buff_size = struct.calcsize(">ihhi")
    length, api_key, api_version, correlation_id = struct.unpack(
        ">ihhi", request[0:buff_size]
    )
    return {
        "length": length,
        "api_key": api_key,
        "api_version": api_version,
        "correlation_id": correlation_id,
    }


def main() -> None:
    server = socket.create_server(("localhost", 9092), reuse_port=True)
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
