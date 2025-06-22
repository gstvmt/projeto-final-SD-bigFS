from kafka import KafkaProducer
from threading import Lock
import Pyro5.api
import threading
import socket
import psutil
import base64
import time
import json
import os

@Pyro5.api.expose
class Worker:

    def __init__(self, kafka_broker='localhost:9092'):
        """
            Inicializa uma instância de Worker responsável por armazenar arquivos e enviar heartbeats ao servidor mestre.

            -------------------------------------------------------
            Funcionamento geral:
                Este construtor configura os recursos locais de armazenamento do worker e inicializa a comunicação
                com o servidor mestre por meio de um broker Kafka.

                O worker também cria uma thread dedicada que irá enviar periodicamente mensagens de heartbeat
                contendo métricas de sistema.

            -------------------------------------------------------
            Principais tarefas executadas durante a inicialização:
                1. Gerar um `worker_id` único para identificação, combinando o nome da máquina (`hostname`) e o ID do processo (`PID`).

                2. Criar a lista `self.opened_files` para rastrear os arquivos abertos pelo worker.

                3. Inicializar um `Lock` (`self.lock`) para proteger o acesso concorrente a recursos compartilhados.

                4. Garantir que o diretório local de armazenamento de arquivos (`self.file_space_path`, padrão `"data/{self.worker_id}/"`) exista.
                   Se o diretório não existir, ele será criado automaticamente.

                5. Configurar o produtor Kafka (`self.kafka_producer`) para permitir o envio de mensagens
                   ao servidor mestre.

                6. Definir a flag `self.running` como `True`, permitindo que o loop de heartbeat execute continuamente.

                7. Iniciar a thread `self.heartbeat_thread`, responsável por executar o método `_send_heartbeat`
                   para envio periódico de informações de saúde do worker.

            -------------------------------------------------------
            Variáveis e atributos importantes:

            - `self.opened_files`:  
                Lista de arquivos atualmente abertos por este worker.

            - `self.lock`:  
                Objeto de sincronização (`threading.Lock`) para proteger seções críticas.

            - `self.file_space_path`:  
                Caminho local onde os arquivos serão armazenados.  
                    Exemplo: `"data/worker_<hostname>_<PID>/"`

            - `self.kafka_producer`:  
                Instância de produtor Kafka configurado para enviar mensagens JSON.

            - `self.worker_id`:  
                Identificador único do worker, no formato:
                    worker_<hostname>_<PID>

            - `self.heartbeat_thread`:  
                Thread que executa `_send_heartbeat` para envio contínuo de heartbeats ao tópico Kafka.

            -------------------------------------------------------
            Parâmetros:
                kafka_broker : str
                    Endereço do broker Kafka usado para enviar os heartbeats.
                    Formato típico: `'host:port'`, exemplo: `'localhost:9092'`.

            -------------------------------------------------------
            Retorno:
                Nenhum (construtor da classe).

            -------------------------------------------------------
            Observações:
                - O diretório de armazenamento `"data/worker_<hostname>_<PID>/"` é criado automaticamente se não existir.
                - O envio de heartbeats começa imediatamente após a inicialização.
        """
        self.worker_id = f"worker_{socket.gethostname()}_{os.getpid()}"
        self.opened_files = []
        self.lock = Lock()
        self.file_space_path= f"data/{self.worker_id}/"  # Local directory for file storage

        if not os.path.exists(self.file_space_path):
            os.makedirs(self.file_space_path)

        # Configuração do Kafka
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.running = True
        
        # Inicia thread de heartbeat
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    def _send_heartbeat(self):
        """
            Envia mensagens periódicas de heartbeat para o Kafka contendo informações sobre o estado do worker.

            -------------------------------------------------------
            Funcionamento geral:
                Esta função roda em loop contínuo enquanto `self.running` for True.
                Ela coleta métricas do sistema operacional (memória, disco, I/O) e envia essas informações para
                o tópico Kafka chamado `'worker_heartbeats'`, permitindo que o servidor mestre monitore a saúde
                dos workers.

            -------------------------------------------------------
            Principais tarefas da função:

                1. Monitorar o uso de memória RAM do sistema em percentual.

                2. Verificar o espaço livre em disco (em gigabytes).

                3. Calcular a taxa de leitura e escrita em disco (I/O) em bytes por segundo,
                comparando a quantidade de bytes lidos e escritos entre dois instantes de tempo.

                4. Construir uma mensagem de heartbeat no formato JSON, contendo todas essas informações.

                5. Enviar a mensagem para o tópico Kafka `'worker_heartbeats'`, para que o servidor mestre possa processá-la.

            -------------------------------------------------------
            Variáveis e estruturas importantes:

            - `self.running`:  
                Flag booleana que controla o loop. Enquanto estiver `True`, a função continua enviando heartbeats.

            - `self.kafka_producer`:  
                Produtor Kafka configurado para publicar mensagens no tópico de heartbeat.

            - `self.worker_id`:  
                String que identifica unicamente este worker.

            - Exemplo de estrutura da mensagem de heartbeat enviada:
                heartbeat_msg = {
                    'worker_id': 'worker_1',
                    'memory_used_percent': 65.2,
                    'disk_free_gb': 120.5,
                    'disk_read_rate_bps': 10240.0,
                    'disk_write_rate_bps': 20480.0,
                }

            -------------------------------------------------------
            Parâmetros:
                Nenhum (além de `self`).

            -------------------------------------------------------
            Retorno:
                Nenhum (função de execução contínua / loop infinito).

            -------------------------------------------------------
            Observações:
                - Caso ocorra alguma exceção durante o envio de mensagens Kafka, a exceção é capturada
                e uma mensagem de erro é impressa no console.  
                O loop então espera 1 segundo antes de tentar novamente.
        """

        last_io_counters = psutil.disk_io_counters()
        last_time = time.time()

        while self.running:
            try:
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

                heartbeat_msg = {
                    'worker_id': self.worker_id,
                    'memory_used_percent': memory_percent_used,
                    'disk_free_gb': round(disk_free_gb, 2),
                    'disk_read_rate_bps': round(read_rate, 2),
                    'disk_write_rate_bps': round(write_rate, 2),
                }

                self.kafka_producer.send('worker_heartbeats', heartbeat_msg)
                time.sleep(1)  # Envia a cada 1 segundo
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
        
    def close_block(self, index):
        """
            Fecha um bloco aberto e o mantém no armazenamento.

            -------------------------------------------------------
            Funcionamento geral:
                Esta função fecha um bloco que estava aberto para leitura/escrita,
                liberando os recursos associados enquanto mantém o bloco armazenado.

            -------------------------------------------------------
            Principais tarefas da função:

                1. Obtém um lock para garantir operação thread-safe.
                
                2. Localiza o arquivo correspondente ao índice fornecido.
                
                3. Fecha o arquivo e remove sua referência da lista de arquivos abertos.

            -------------------------------------------------------
            Variáveis e estruturas importantes:

            - `self.lock`: Objeto de lock para sincronização de threads.
            - `self.opened_files`: Dicionário que mapeia índices para objetos de arquivo abertos.
                Formato:
                    self.opened_files = {
                        <index>: <file_object>,
                        ...
                    }

            -------------------------------------------------------
            Parâmetros:
                :param index: (int) Índice do arquivo a ser fechado (retornado por open_file).

            -------------------------------------------------------
            Retorno:
                None

        """
        try:
            with self.lock:
                file = self.opened_files[index]
                file.close()
                self.opened_files.pop(index)
                return
            
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
    daemon = Pyro5.server.Daemon()
    w = Worker()
    nome = w.worker_id
    print(nome)
    uri = daemon.register(w)

    ns = Pyro5.api.locate_ns(host="0.0.0.0", port=9090)
    server_uri = ns.lookup("fs_server")
    server = Pyro5.api.Proxy(server_uri)
    server.register_worker(nome, uri)

    print(f"Worker {nome} iniciado.")
    daemon.requestLoop()

if __name__ == "__main__":
    main()
