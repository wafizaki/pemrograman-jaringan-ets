import socket
import logging
from concurrent.futures import ThreadPoolExecutor
from file_protocol import FileProtocol
import argparse

fp = FileProtocol()

def handle_client(connection, address):
    logging.warning(f"Client connected: {address}")
    try:
        buffer = ""
        while True:
            data = connection.recv(4096)
            if not data:
                break
            buffer += data.decode()
            while "\r\n\r\n" in buffer:
                request, buffer = buffer.split("\r\n\r\n", 1)
                response = fp.proses_string(request.strip())
                connection.sendall((response + "\r\n\r\n").encode())
    except Exception as e:
        logging.error(f"Error handling client {address}: {e}")
    finally:
        connection.close()
        logging.warning(f"Connection closed: {address}")

def main(worker_count):
    logging.warning(f"Server starting with thread pool (workers={worker_count})...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", 6664))
        server.listen(10)
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            while True:
                conn, addr = server.accept()
                executor.submit(handle_client, conn, addr)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="File server with configurable worker pool")
    parser.add_argument('--workers', type=int, default=50, help='Number of worker threads')
    args = parser.parse_args()

    main(args.workers)
