# datanode.py - Classe DataNodeService modificada

import os
import Pyro5.api
import socket
import time
import threading
import json
import base64
from kafka import KafkaProducer

# --- Configuração do DataNode ---

HEARTBEAT_INTERVAL_SECONDS = 15
KAFKA_TOPIC = "datanode_heartbeats"
KAFKA_SERVERS = ['localhost:9092'] 

#-----------------------------------

@Pyro5.api.expose
class DataNodeService:
    def __init__(self, node_id, storage_path):
        self._node_id = node_id
        self._storage_path = storage_path
        
        # Garante que o diretório de armazenamento exista
        if not os.path.exists(self._storage_path):
            os.makedirs(self._storage_path)
            print(f"[{self._node_id}] Diretório de armazenamento criado em: {self._storage_path}")
        else:
            print(f"[{self._node_id}] Utilizando diretório de armazenamento: {self._storage_path}")

    def _get_block_filepath(self, block_id: str) -> str:
        """
        Gera um caminho de arquivo seguro para um block_id, prevenindo ataques
        de path traversal.
        """
        # Garante que o block_id não contenha caracteres como '..' ou '/'
        safe_filename = os.path.basename(block_id)
        return os.path.join(self._storage_path, safe_filename)

    def write_block(self, block_id, data):
        data = base64.b64decode(data.get("data")) if isinstance(data, dict) else data
        filepath = self._get_block_filepath(block_id)
        print(f"[{self._node_id}] Escrevendo bloco '{block_id}' em disco ({len(data)} bytes).")
        try:
            with open(filepath, "wb") as f: # 'wb' = write binary
                f.write(data)
            return True
        except IOError as e:
            print(f"ERRO ao escrever bloco '{block_id}' em disco: {e}")
            return False

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
        

def heartbeat_producer(datanode_uri):

    """Envia heartbeats para o tópico Kafka em uma thread de background."""

    producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    payload = {"address": str(datanode_uri)}
    print(f"Thread de Heartbeat iniciada para {datanode_uri}.")

    while True:

        payload["timestamp"] = time.time()
        producer.send(KAFKA_TOPIC, payload)
        time.sleep(HEARTBEAT_INTERVAL_SECONDS) 

if __name__ == "__main__":

# Usamos o nome do host para ter um ID único se rodarmos vários na mesma máquina
    node_id = f"datanode-{socket.gethostname()}-{time.time_ns()}"
    
    daemon = Pyro5.server.Daemon()
    datanode_service = DataNodeService(node_id, storage_path=node_id)
    uri = daemon.register(datanode_service)

    heartbeat_thread = threading.Thread(target=heartbeat_producer, args=(uri,))
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

    print("="*50)
    print(f"✅ DataNode '{node_id}' pronto e ouvindo em: {uri}")
    print("="*50)
    daemon.requestLoop() 