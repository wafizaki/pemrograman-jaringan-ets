import socket
import logging
import multiprocessing
from file_protocol import FileProtocol
import argparse

def handle_client(conn, addr):
    logging.warning(f"Client connected: {addr}")
    try:
        buffer = ""
        fp = FileProtocol()
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buffer += data.decode()
            while "\r\n\r\n" in buffer:
                request, buffer = buffer.split("\r\n\r\n", 1)
                response = fp.proses_string(request.strip())
                conn.sendall((response + "\r\n\r\n").encode())
    except Exception as e:
        logging.error(f"Error handling client {addr}: {e}")
    finally:
        conn.close()
        logging.warning(f"Connection closed: {addr}")

def worker_process(server_socket):
    while True:
        conn, addr = server_socket.accept()
        handle_client(conn, addr)

def main(num_workers):
    logging.basicConfig(level=logging.WARNING, format='%(processName)s - %(levelname)s - %(message)s')
    logging.warning(f"Server starting with multiprocessing prefork (workers={num_workers})...")

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("0.0.0.0", 6663))
    server_socket.listen(10)

    workers = []

    # Fork worker processes
    for _ in range(num_workers):
        p = multiprocessing.Process(target=worker_process, args=(server_socket,))
        p.daemon = True  # proses mati otomatis saat main exit
        p.start()
        workers.append(p)

    # Tunggu worker berjalan
    try:
        for p in workers:
            p.join()
    except KeyboardInterrupt:
        logging.warning("Server shutting down...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="File server with configurable process pool")
    parser.add_argument('--workers', type=int, default=4, help='Number of worker processes')
    args = parser.parse_args()

    main(args.workers)
