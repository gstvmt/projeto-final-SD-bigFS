from kafka import KafkaProducer
from threading import Lock
import Pyro5.api
import threading
import socket
import base64
import time
import json
import os

@Pyro5.api.expose
class Worker:

    def __init__(self, kafka_broker='localhost:9092'):
        self.opened_files = []
        self.lock = Lock()
        self.file_space_path= "data/"  # Local directory for file storage

        if not os.path.exists(self.file_space_path):
            os.makedirs(self.file_space_path)

        # Configuração do Kafka
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.worker_id = f"worker_{socket.gethostname()}_{os.getpid()}"
        self.running = True
        
        # Inicia thread de heartbeat
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    def _send_heartbeat(self):
        """Envia mensagens periódicas de heartbeat para o Kafka"""
        while self.running:
            try:
                heartbeat_msg = {
                    'worker_id': self.worker_id,
                    'timestamp': time.time(),
                    'status': 'alive',
                    'load': len(self.opened_files)  # Métrica simples de carga
                }
                self.kafka_producer.send('worker_heartbeats', heartbeat_msg)
                time.sleep(2)  # Envia a cada 2 segundos
            except Exception as e:
                print(f"Erro ao enviar heartbeat: {e}")
                time.sleep(1)

    def shutdown(self):
        """Para o envio de heartbeats"""
        self.running = False
        self.heartbeat_thread.join()
        self.kafka_producer.close()
    
    def open_file(self, hash, mode):
        """
        Open a file in the local file space.
        :param hash: The hash of the file to open.
        :param mode: The mode in which to open the file (e.g., 'rb', 'wb').
        :return: The opened file object.
        """
        try:
            with self.lock:
                file = open(f"{self.file_space_path}{hash}", mode)
                self.opened_files.append(file)
                index = len(self.opened_files) - 1
                return index
        except Exception as e:
            raise e
        
    def write_chunk(self, chunk, index):
        """
        Write a chunk of data to the opened file.
        :param chunk: The chunk of data to write.
        :param index: The index of the opened file.
        :return: True if EOF, False otherwise.
        """
        try:
            chunk = base64.b64decode(chunk.get("data"))
            with self.lock:
                file = self.opened_files[index]
                if not chunk:
                    file.close()
                    self.opened_files.pop(index)
                    return True
                
                file.write(chunk)
                return False
        except Exception as e:
            raise e
        
    def read_chunks(self, index):
        """
        Read data chunks from the opened file.
        :param index: The index of the opened file.
        :return: The chunk of data read from the file.
        """
        try:
            with self.lock:
                file = self.opened_files[index]
                while True:
                    chunk = file.read(1024*64)
                    if not chunk:
                        file.close()
                        self.opened_files.pop(index)
                        break
                    yield chunk
                
        except Exception as e:
            raise e
        
    def rm(self, hash):
        """
        Remove a file from the local file space.
        :param hash: The hash of the file to remove.
        """
        try:
            with self.lock:
                file_path = f"{self.file_space_path}{hash}"
                if os.path.exists(file_path):
                    os.remove(file_path)
                else:
                    raise Exception("File not found")
        except Exception as e:
            raise e
        
        
       

def main():
    nome = f"worker_{socket.gethostname()}"
    print(nome)
    daemon = Pyro5.server.Daemon()
    w = Worker()
    uri = daemon.register(w)

    ns = Pyro5.api.locate_ns(host="0.0.0.0", port=9090)
    server_uri = ns.lookup("fs_server")
    server = Pyro5.api.Proxy(server_uri)
    server.register_worker(nome, uri)

    print(f"Worker {nome} iniciado.")
    daemon.requestLoop()

if __name__ == "__main__":
    main()
