import socket
import struct
import threading
import enum
FETCH = 1
VERSIONS = 18
def api_key_entry(api_key: int, min: int, max: int):
    entry = api_key.to_bytes(2) + min.to_bytes(2) + max.to_bytes(2) + b"\x00"
    return entry
def parse_request(request: bytes):
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

def create_response_versions(
    correlation_id: int, api_key: int = 0, error_code: int = 0
):
    response_header = correlation_id.to_bytes(4, byteorder="big", signed=True)
    throttle_time_ms = 0
    tag_buffer = b"\x00"
    print(
        f"api_key: {api_key}, error_code: {error_code}, correlation_id: {correlation_id}"
    )
    response_body = (
        error_code.to_bytes(2)
        + int(3).to_bytes(1)
        + api_key_entry(VERSIONS, 0, 4)
        + api_key_entry(FETCH, 0, 16)
        + throttle_time_ms.to_bytes(4)
        + tag_buffer
    )
    response_len = len(response_header) + len(response_body)
    return response_len.to_bytes(4) + response_header + response_body
def create_response_fetch(correlation_id: int, api_key: int = 0, error_code: int = 0):
    response_header = correlation_id.to_bytes(4, byteorder="big", signed=True)
    throttle_time_ms = 0
    tag_buffer = b"\x00"
    session_id = 0
    responses = []
    print(
        f"api_key: {api_key}, error_code: {error_code}, correlation_id: {correlation_id}"
    )
    response_body = (
        throttle_time_ms.to_bytes(4)
        + error_code.to_bytes(2)
        + session_id.to_bytes(4)
        + tag_buffer
        + int(len(responses) + 1).to_bytes(1)
        + tag_buffer
    )
    response_len = len(response_header) + len(response_body)
    return response_len.to_bytes(4) + response_header + response_body
def handle_client(client_socket, client_address):
    try:
        while True:
            try:
                data = client_socket.recv(1024)
                print(f"Received data from {client_address}")
                if data:
                    request = parse_request(data)
                    print(f"Request: {request}")
                    if 0 <= request["api_version"] <= 18:
                        if request["api_key"] == FETCH:
                            response = create_response_fetch(
                                request["correlation_id"], request["api_key"], 0
                            )
                        else:
                            response = create_response_versions(
                                request["correlation_id"], request["api_key"], 0
                            )
                    else:
                        if request["api_key"] == FETCH:
                            response = create_response_fetch(
                                request["correlation_id"], request["api_key"], 35
                            )
                        else:
                            response = create_response_versions(
                                request["correlation_id"], request["api_key"], 35
                            )
                    client_socket.send(response)
            except Exception as e:
                print(f"Error: {e}")
    finally:
        print(f"Client disconnected at address {client_address}")
        client_socket.close()
def tcp_server():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server listening on port 9092...")
    try:
        while True:
            client_socket, client_address = server.accept()
            client_thread = threading.Thread(
                target=handle_client, args=(client_socket, client_address)
            )
            client_thread.start()
    except KeyboardInterrupt:
        print("Server stopped")
        server.close()
def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")
    tcp_server()
if __name__ == "__main__":
    main()