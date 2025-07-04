from kafka import KafkaProducer
import threading
import Pyro5.api
import socket
import psutil
import base64
import time
import json
import os

# --- Constantes de Configuração ---
HEARTBEAT_INTERVAL_SECONDS = 0.5
KAFKA_TOPIC = "datanode_heartbeats"
KAFKA_SERVERS = ['localhost:9092'] 
#-----------------------------------

@Pyro5.api.expose
class DataNodeService:
    
    def __init__(self, node_id, storage_path):
        """
            Inicializa o datanode.

            -------------------------------------------------------
            Funcionamento geral:
                Configura o ambiente de armazenamento local para os blocos de dados,
                criando o diretório especificado se ele não existir.

            -------------------------------------------------------
            Parâmetros:
                node_id : str
                    Identificador único para este nó de armazenamento.

                storage_path : str
                    Caminho local onde os blocos serão armazenados.

        """
        self._node_id = node_id
        self._storage_path = storage_path
        self.opened_files = {}
        
        # Garante que o diretório de armazenamento exista
        if not os.path.exists(self._storage_path):
            os.makedirs(self._storage_path)
            print(f"[{self._node_id}] Diretório de armazenamento criado em: {self._storage_path}")
        else:
            print(f"[{self._node_id}] Utilizando diretório de armazenamento: {self._storage_path}")


    def _get_block_filepath(self, block_id: str) -> str:
        """
            Obtém o caminho completo para um bloco de dados.

            -------------------------------------------------------
            Funcionamento geral:
                Constrói o caminho completo para o arquivo que armazena o bloco,
                criando a estrutura de diretórios necessária se ela não existir.

            -------------------------------------------------------
            Parâmetros:
                block_id : str
                    Identificador único do bloco de dados.

            -------------------------------------------------------
            Retorno:
                str
                    Caminho completo do arquivo que armazena o bloco.

        """
        # Junta o storage_path com o block_id (permitindo subdiretórios dentro do block_id)
        full_path = os.path.join(self._storage_path, block_id)
        
        # Garante que o caminho gerado esteja dentro da raiz e exista
        full_path = os.path.abspath(full_path)
        if not os.path.exists(full_path):
            print(f"Criando arquivos full_path: {full_path}")
            os.makedirs(os.path.dirname(full_path), exist_ok=True)

        return full_path

    def write_block(self, block_id, data):
        """
            Escreve em um bloco.

            -------------------------------------------------------
            Funcionamento geral:
                Esta função recebe dados e grava no sistema de arquivos local. O bloco é 
                armazenado em um arquivo específico identificado pelo `block_id`.

                Se o arquivo correspondente ao `block_id` não estiver aberto, ele será criado/aberto
                em modo binário. Se o bloco de dados estiver vazio, o arquivo será fechado.

            -------------------------------------------------------
            Parâmetros:
                block_id : str
                    Identificador único do bloco de dados que será gravado.

                data : dict ou bytes
                    Dados a serem escritos no arquivo. Pode ser:
                        - Um dicionário contendo {"data": "base64_encoded_data"}
                        - Dados binários diretamente

            -------------------------------------------------------
            Retorno:
                bool
                    Retorna `True` se a operação for bem-sucedida.

        """

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
        """
            Fecha um bloco de dados aberto anteriormente e o remove da lista de arquivos abertos.

            -------------------------------------------------------
            Funcionamento geral:
                Esta função finaliza o acesso a um bloco de dados que estava sendo manipulado,
                fechando o arquivo correspondente e liberando os recursos do sistema.
                O bloco permanece armazenado no sistema de arquivos local após o fechamento.

            --------------------------------------------
            Parâmetros:
                block_id : str
                    Identificador único do bloco de dados que será fechado.

            -------------------------------------------------------
            Retorno:
                None
                    A função não retorna nenhum valor explicitamente.

        """
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
        """
            Lê o conteúdo de um bloco de dados do armazenamento local.

            -------------------------------------------------------
            Funcionamento geral:
                Esta função recupera o conteúdo completo de um bloco de dados armazenado no sistema
                de arquivos local, identificado pelo `block_id`. O arquivo é aberto em modo binário
                e seu conteúdo é lido integralmente.

            -------------------------------------------------------
            Parâmetros:
                block_id : str
                    Identificador único do bloco de dados que será lido.

            -------------------------------------------------------
            Retorno:
                bytes
                    Conteúdo binário do bloco de dados lido.

        """

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
    """
        Envia métricas do nó para o tópico Kafka periodicamente.

        -------------------------------------------------------
        Funcionamento geral:
            Executa em loop infinito, coletando métricas do sistema (uso de memória,
            espaço em disco e taxas de I/O) e enviando para um tópico Kafka.

        -------------------------------------------------------
        Parâmetros:
            datanode_uri : str
                Endereço URI do datanode que está enviando os heartbeats.

            node_id : str
                Identificador único do nó que está enviando os heartbeats.

        -------------------------------------------------------
        Comportamento:
            - Coleta métricas a cada HEARTBEAT_INTERVAL_SECONDS segundos
            - Calcula taxas de leitura/escrita do disco entre intervalos
            - Envia payload contendo:
                * Endereço do nó
                * Uso de memória (percentual)
                * Espaço livre em disco (GB)
                * Taxas de leitura/escrita do disco (bytes/segundo)
    """

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