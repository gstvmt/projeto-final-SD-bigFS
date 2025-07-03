import os
import Pyro5.api
import socket
import time
import threading
import json
import psutil
import base64
from kafka import KafkaProducer

# --- Constantes de Configuração ---
HEARTBEAT_INTERVAL_SECONDS = 0.5
KAFKA_TOPIC = "datanode_heartbeats"
KAFKA_SERVERS = ['localhost:9092'] 
#-----------------------------------

@Pyro5.api.expose
class DataNodeService:
    
    def __init__(self, node_id, storage_path):
        self._node_id = node_id
        self._storage_path = storage_path
        self.opened_files = {}
        
        # Garante que o diretório de armazenamento exista
        if not os.path.exists(self._storage_path):
            os.makedirs(self._storage_path)
            print(f"[{self._node_id}] Diretório de armazenamento criado em: {self._storage_path}")
        else:
            print(f"[{self._node_id}] Utilizando diretório de armazenamento: {self._storage_path}")

# ========================================= Metodos ================================================

    def _get_block_filepath(self, block_id: str) -> str:

        # Junta o storage_path com o block_id (permitindo subdiretórios dentro do block_id)
        full_path = os.path.join(self._storage_path, block_id)
        
        # Garante que o caminho gerado esteja dentro da raiz e exista
        full_path = os.path.abspath(full_path)
        if not os.path.exists(full_path):
            print(f"Criando arquivos full_path: {full_path}")
            os.makedirs(os.path.dirname(full_path), exist_ok=True)

        return full_path

    def write_block(self, block_id, data):

        data = base64.b64decode(data.get("data")) if isinstance(data, dict) else data

        filepath = self._get_block_filepath(block_id)

        try:
            if not filepath in self.opened_files:
                file = open(filepath, "wb")
                self.opened_files[filepath] = file

            file = self.opened_files[filepath]
            if not data:
                file.close()
                self.opened_files.pop(filepath)
                return True
            
            file.write(data)
            return True
        except Exception as e:
            raise e


    def close_block(self, block_id):
        
        try:
            filepath = self._get_block_filepath(block_id)

            file = self.opened_files[filepath]
            file.close()
            self.opened_files.pop(filepath)

            print(f"[{self._node_id}] Bloco '{block_id}' fechado e mantido no armazenamento.")
            return
            
        except Exception as e:
            raise e
        
    def read_block(self, block_id):
        filepath = self._get_block_filepath(block_id)
        print(f"[{self._node_id}] Lendo bloco '{block_id}' do disco.")
        try:
            with open(filepath, "rb") as f: # 'rb' = read binary
                return f.read()
        except FileNotFoundError:
            raise KeyError(f"Bloco '{block_id}' não encontrado neste nó.")
        except IOError as e:
            print(f"ERRO ao ler bloco '{block_id}' do disco: {e}")
            raise

    def delete_block(self, block_id):
        filepath = self._get_block_filepath(block_id)
        print(f"[{self._node_id}] Deletando bloco '{block_id}' do disco.")
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
            return True
        except IOError as e:
            print(f"ERRO ao deletar bloco '{block_id}' do disco: {e}")
            return False

# ========================================= Heartbeat Producer Thread ================================================

def heartbeat_producer(datanode_uri, node_id):

    """Envia heartbeats para o tópico Kafka em uma thread de background."""

    producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    payload = {"address": str(datanode_uri)}
    print(f"Thread de Heartbeat iniciada para {datanode_uri}.")

    last_io_counters = psutil.disk_io_counters()
    last_time = time.time()
    while True:
        
        # --- Calcular taxa de IO ---
        current_io_counters = psutil.disk_io_counters()
        current_time = time.time()
        elapsed = current_time - last_time

        read_rate = (current_io_counters.read_bytes - last_io_counters.read_bytes) / elapsed  # bytes por segundo
        write_rate = (current_io_counters.write_bytes - last_io_counters.write_bytes) / elapsed  # bytes por segundo

        last_io_counters = current_io_counters
        last_time = current_time

        # --- Capturar memória ---
        memory = psutil.virtual_memory()
        memory_percent_used = memory.percent

        # --- Capturar espaço em disco ---
        disk = psutil.disk_usage('/')
        disk_free_gb = disk.free / (1024 ** 3)

        # --- Montar payload ---
        payload["node_name"] = node_id
        payload["memory_used_percent"] = memory_percent_used
        payload["disk_free_gb"] = round(disk_free_gb, 2)
        payload["disk_read_rate_bps"] = round(read_rate, 2)
        payload["disk_write_rate_bps"] = round(write_rate, 2)
        
        
        producer.send(KAFKA_TOPIC, payload)
        time.sleep(HEARTBEAT_INTERVAL_SECONDS) 

# ========================================= Main ================================================

if __name__ == "__main__":

    # Usamos o nome do host para ter um ID único se rodarmos vários na mesma máquina
    node_id = f"datanode-{socket.gethostname()}-{time.time_ns()}"
    
    daemon = Pyro5.server.Daemon()
    datanode_service = DataNodeService(node_id, storage_path=node_id)
    uri = daemon.register(datanode_service)

    heartbeat_thread = threading.Thread(target=heartbeat_producer, args=(uri, node_id))
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

    print("="*50)
    print(f"DataNode '{node_id}' pronto e ouvindo em: {uri}")
    print("="*50)
    daemon.requestLoop() 