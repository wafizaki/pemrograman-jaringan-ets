import socket
import base64
import os
import time
import csv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

server_address = ('172.20.0.4', 6665)

files = {
    '10MB': '10MB.dat',
    '50MB': '50MB.dat',
    '100MB': '100MB.dat'
}

operations = ['upload', 'download']
client_pool_sizes = [1, 5, 50]

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

def send_command(command_str):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(60)
            sock.connect(server_address)
            sock.sendall(command_str.encode() + b'\r\n\r\n')
            return receive_response(sock)
    except Exception as e:
        logging.error(f"send_command error: {e}")
        return None

def receive_response(sock):
    data_received = ""
    try:
        while True:
            data = sock.recv(4096)
            if data:
                data_received += data.decode()
                if "\r\n\r\n" in data_received:
                    break
            else:
                break
        import json
        return json.loads(data_received.strip())
    except Exception as e:
        logging.error(f"receive_response error: {e}")
        return None

def remote_upload(filepath):
    if not os.path.exists(filepath):
        logging.error(f"File not found: {filepath}")
        return False

    filename = os.path.basename(filepath)
    chunk_size = 8192
    try:
        with open(filepath, 'rb') as f, socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(60)
            sock.connect(server_address)
            offset = 0
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                b64_chunk = base64.b64encode(chunk).decode()
                command = f"UPLOAD_CHUNK {filename} {b64_chunk} {offset}\r\n\r\n"
                sock.sendall(command.encode())
                hasil = receive_response(sock)
                if not hasil or hasil.get('status') != 'OK':
                    logging.warning(f"Upload failed at offset {offset}")
                    return False
                offset += len(chunk)
        return True
    except Exception as e:
        logging.error(f"remote_upload error: {e}")
        return False

def remote_download(filename):
    try:
        hasil = send_command(f"GET {filename}")
        return hasil and hasil.get('status') == 'OK'
    except Exception as e:
        logging.error(f"remote_download error: {e}")
        return False

def remote_list():
    try:
        hasil = send_command("LIST")
        return hasil and hasil.get('status') == 'OK'
    except Exception as e:
        logging.error(f"remote_list error: {e}")
        return False

def stress_test(op, vol, client_pool):
    filename = files.get(vol, '')
    total_bytes = os.path.getsize(filename) if filename and os.path.exists(filename) else 1
    success = 0
    failure = 0
    start_time = time.time()

    def task(i):
        nonlocal success, failure
        logging.info(f"Worker #{i+1} starting operation '{op}' with volume '{vol}'")
        try:
            if op == 'upload':
                res = remote_upload(filename)
            elif op == 'download':
                res = remote_download(filename)
            elif op == 'list':
                res = remote_list()
            else:
                logging.error(f"Unknown operation: {op}")
                res = False

            if res:
                success += 1
                logging.info(f"Worker #{i+1} SUCCESS")
            else:
                failure += 1
                logging.warning(f"Worker #{i+1} FAILED")
        except Exception as e:
            failure += 1
            logging.error(f"Worker #{i+1} ERROR: {e}")

    with ThreadPoolExecutor(max_workers=client_pool) as executor:
        futures = [executor.submit(task, i) for i in range(client_pool)]
        for _ in as_completed(futures):
            pass

    end_time = time.time()
    total_time = end_time - start_time
    avg_time_per_client = total_time / client_pool if client_pool > 0 else 0
    throughput = total_bytes / avg_time_per_client if avg_time_per_client > 0 else 0

    logging.info(f"Operation '{op}', volume '{vol}', clients {client_pool} finished: "
                 f"{success} success, {failure} failure, avg_time {avg_time_per_client:.2f}s, throughput {throughput:.2f} B/s")

    return avg_time_per_client, throughput, success, failure

def run_all_tests():
    # Set server pool size manual di sini
    current_server_pool = 1  # ganti sesuai setting server-mu

    with open("thread_1.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "Nomor", "Operasi", "Volume", "Jumlah client worker pool", "Jumlah server worker pool",
            "Waktu total per client", "Throughput per client (bytes/s)",
            "Jumlah client sukses", "Jumlah client gagal"
        ])

        nomor = 1
        for op in operations:
            for vol in files if op != 'list' else ['10MB']:
                for client_pool in client_pool_sizes:
                    logging.warning(f"\n[# {nomor}] === Mulai kombinasi ===")
                    logging.warning(f"    Operasi      : {op}")
                    logging.warning(f"    Volume       : {vol}")
                    logging.warning(f"    Client Pool  : {client_pool}")
                    logging.warning(f"    Server Pool  : {current_server_pool}")
                    start_combo = time.time()

                    avg_time, tput, succ, fail = stress_test(op, vol, client_pool)

                    duration = round(time.time() - start_combo, 2)
                    logging.warning(f"[# {nomor}] === Selesai dalam {duration} detik ===")

                    writer.writerow([
                        nomor, op, vol, client_pool, current_server_pool,
                        round(avg_time, 4), round(tput, 2), succ, fail
                    ])
                    nomor += 1


if __name__ == '__main__':
    run_all_tests()
