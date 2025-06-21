'''
Esse arquivo busca implementar o servidor mestre do sistema de arquivos distribuido.
Ele sera responsavel por gerenciar os nos do sistema distribuido e os clientes.
'''
from kafka import KafkaConsumer
from uhashring import HashRing
from pathlib import Path
import threading
import Pyro5.api
import hashlib
import json
import time

@Pyro5.api.expose
class ServerMaster():

    def __init__(self, kafka_broker='localhost:9092'):
        """
            Inicializa a instância do servidor mestre responsável por gerenciar os workers e os usuários do sistema.

            -------------------------------------------------------
            Principais funções executadas durante a inicialização:

            1. Cria as estruturas de dados que armazenam informações sobre:
                - Workers ativos e disponíveis.
                - Mapeamento de arquivos entre workers.
                - Usuários cadastrados no sistema.
            
            2. Configura a conexão com o broker Kafka para receber mensagens de heartbeat enviadas pelos workers.

            3. Inicia uma thread separada que ficará responsável por monitorar periodicamente o status dos workers,
            processando os heartbeats recebidos via Kafka.

            -------------------------------------------------------
            Parâmetros:
            kafka_broker : str
                Endereço do broker Kafka utilizado para comunicação com os workers.
                Formato típico: `"host:port"`, por exemplo: `"localhost:9092"`.

            -------------------------------------------------------
            Estruturas e atributos criados:
            - `self.workers`:  
                Dicionário para armazenar proxies ou informações dos workers conectados.  
                Formato:  
                    {worker_id: <Proxy Pyro5 ou objeto worker>}

            - `self.hash_ring`:  
                Estrutura (inicialmente `None`) que pode ser usada futuramente para balanceamento de carga com **Consistent Hashing**.

            - `self.files` e `self.files_map`:  
                Estruturas para mapear os arquivos gerenciados no sistema:
                    - `self.files`: Mapeia o caminho original do arquivo para seu hash.
                    - `self.files_map`: Mapeia o hash do arquivo para o worker onde ele está armazenado.

            - `self.users`:  
                Dicionário simples de usuários, inicialmente com um usuário padrão:
                    {'admin': 'admin'}

            - `self.kafka_consumer`:  
                    Instância de um consumidor Kafka configurado para escutar o tópico **`worker_heartbeats`**,  
                    deserializando mensagens no formato JSON.

            - `self.last_heartbeats`:  
                Dicionário que armazena informações de monitoramento de cada worker, como último timestamp de heartbeat e uso de recursos.

            - `self.monitor_thread`:  
                Thread em segundo plano que executa o método `_monitor_workers`, responsável por monitorar os heartbeats dos workers.

            -------------------------------------------------------
            Métodos chamados:
            - `self.load()`:  
                Método responsável por carregar os usuários previamente registrados no sistema.

            -------------------------------------------------------
            Retorno:
            Nenhum (construtor da classe).

        """
        # dicionario de workers
        self.workers = {}

        # hash ring para o balanceamento de carga entre os workers
        self.hash_ring = None
        
        # mapa de arquivos
        # TODO: substituir esse metodo por algo que suporte maiores cargas 
        #       (talvez um banco de dados local)
        self.files = {} #path : hash path
        self.files_map = {}  #hash path : worker_name

        # dicionario de usuarios
        self.users = {'admin': 'admin'}
        '''
            O caminho do usuario será --> /NFS/user_name
        '''
        self.load() # carrega os usuarios do servidor mestre

        # Configuração do Kafka
        self.kafka_consumer = KafkaConsumer(
            'worker_heartbeats',
            bootstrap_servers=[kafka_broker],
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Dicionário para armazenar últimos heartbeats
        self.last_heartbeats = {}
        
        # Inicia thread de monitoramento
        self.monitor_thread = threading.Thread(target=self._monitor_workers)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

    def _monitor_workers(self):
        """
            Consome mensagens de heartbeat vindas do Kafka e monitora o status dos workers.

            -------------------------------------------------------
            Funcionamento geral:
                Esta função roda em loop contínuo, escutando o tópico Kafka configurado para `self.kafka_consumer`.
                Cada mensagem recebida contém informações de um worker, incluindo seu `worker_id`, uso de memória,
                espaço livre em disco e taxa de I/O.

            -------------------------------------------------------
            Principais tarefas da função:

                1. Atualizar o dicionário `self.last_heartbeats`, armazenando o timestamp atual e os dados de recursos
                do worker que acabou de enviar o heartbeat.
                
                2. Verificar a cada ciclo se existem workers inativos (ou seja, que não enviam heartbeat há mais de 5 segundos).
                Caso algum worker seja considerado inativo, seu status é alterado para `'off'` no dicionário `self.last_heartbeats`.
                
                3. Exibir logs no console com o status dos workers ativos e inativos.

            -------------------------------------------------------
            Variáveis e estruturas importantes:

            - `self.kafka_consumer`: Consumidor Kafka configurado para ler mensagens de heartbeat dos workers.
            - `self.last_heartbeats`: Dicionário que mantém o último status conhecido de cada worker.
                Formato:
                    self.last_heartbeats = {
                        'worker_id_1': {
                            'timestamp': <última vez que enviou heartbeat>,
                            'status': 'on' ou 'off',
                            'memory_used_percent': <uso de memória em %>,
                            'disk_free_gb': <espaço livre em GB>,
                            'disk_read_rate_bps': <taxa de leitura em bytes por segundo>,
                            'disk_write_rate_bps': <taxa de escrita em bytes por segundo>
                        },
                        ...
                }
                ```

            -------------------------------------------------------
            Parâmetros:
                Nenhum (além de `self`).

            -------------------------------------------------------
            Retorno:
                Nenhum (função de execução contínua / loop infinito).

        """
        for message in self.kafka_consumer:
            try:
                data = message.value
                worker_id = data['worker_id']
                
                current_time = time.time()
                # Atualiza último heartbeat recebido

                self.last_heartbeats[worker_id] = {
                    'timestamp': current_time,
                    'status': 'on',
                    'memory_used_percent': data['memory_used_percent'],
                    'disk_free_gb': data['disk_free_gb'],
                    'disk_read_rate_bps': data['disk_read_rate_bps'],
                    'disk_write_rate_bps': data['disk_write_rate_bps']
                }

                # Verifica workers inativos (mais de 5 segundos sem heartbeat)
                inactive_workers = [
                    wid for wid, info in self.last_heartbeats.items()
                    if (current_time - info['timestamp'] > 5 and info['status'] == 'on')
                ]

                # Remove workers inativos
                for wid in inactive_workers:
                    print(f"Worker {wid} marcado como off")
                    self.last_heartbeats[wid]['status'] = 'off'

            except Exception as e:
                print(f"Erro ao processar heartbeat: {e}")

    def register_worker(self, name, uri):
        '''
        Registra um worker no servidor mestre.
        :param name: nome do worker
        :param uri: uri do worker
        '''
        proxy = Pyro5.api.Proxy(uri)
        
        self.workers[name] = proxy
        self.hash_ring = HashRing(list(self.workers.keys()))
        print(f"Worker {name} registrado com sucesso.")

    def register_user(self, user, password):
        '''
        Registra um usuario no servidor mestre.
        :param user: nome do usuario
        :param password: senha do usuario
        '''
        if user in self.users:
            raise Exception("Usuario ja existe")
        else:
            self.users[user] = password
            self.save_user(user, password)
            return "/NFS/" + user
        
    def login_user(self, user, password):
        '''
        Faz o login de um usuario no servidor mestre.
        :param user: nome do usuario
        :param password: senha do usuario
        '''
        if user in self.users and self.users[user] == password:
            return "/NFS/" + user
        else:
            raise Exception("Usuario ou senha invalidos")
        
    def save_user(self, user, password):
        '''
        Salva um usuario no servidor mestre.
        :param user: nome do usuario
        :param password: senha do usuario
        '''
        with open("user_table.txt", "a") as f:
            f.write(f"{user}:{password}\n")

    def load(self):
        '''
        Carrega os metadados do servidor.
        '''
        try:
            with open("user_table.txt", "r") as f:
                for line in f:
                    user, password = line.strip().split(":")
                    self.users[user] = password
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"Erro ao carregar o arquivo de usuarios: {e}")
        
        try:
            with open("files.txt", "r") as f:
                for line in f:
                    path, hash = line.strip().split(":")
                    self.files[path] = hash
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"Erro ao carregar o arquivo de usuarios: {e}")

        try:
            with open("files_map.txt", "r") as f:
                for line in f:
                    hash, worker_name = line.strip().split(":")
                    self.files_map[hash] = worker_name
            self.hash_ring = HashRing(nodes=list(self.workers.keys()), replicas=3)
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"Erro ao carregar o arquivo de usuarios: {e}")

    def ls(self, path, user):
        '''
        Lista os arquivos e diretorios de um caminho.
        :param path: caminho a ser listado
        '''
        try:
            ret_set = set()
            path = Path(path)

            # verifica se o caminho esta no formato correto
            if not path.is_absolute():
                path = Path(f"/NFS/{user}") / path

            p_string = str(path)
            if not p_string.endswith('/'):
                p_string += '/'

            for p in self.files.keys():
                if p.startswith(p_string):
                    
                    # Verifica se há partes antes de acessar
                    relative = Path(p).relative_to(path)
                    if len(relative.parts) > 0:
                        ret_set.add(relative.parts[0])

            return ret_set
        except Exception as e:
            raise e
        
    def cp_from(self, path, user):
        '''
        Copia um arquivo de um cliente para um worker.
        :param path: caminho do arquivo de destino
        '''
        try:
            # verifica se o caminho esta no formato correto
            path = Path(path)
            if not path.is_absolute():
                path = Path(f"/NFS/{user}") / path

            p_string = str(path)

            # verifica se o arquivo nao existe
            if p_string not in self.files.keys():
                hash = hashlib.sha256(p_string.encode()).hexdigest()
                self.files[p_string] = hash
                self.files_map[hash] = self.hash_ring.get_node(hash)
                self.save_file()

            path_hash = self.files[p_string]
            worker_name = self.files_map[path_hash]
            worker = self.workers[worker_name]
            worker._pyroClaimOwnership()
            index = worker.open_file(path_hash, "wb")
            endpoint = (index, worker_name) # indice do arquivo aberto no worker e o nome do worker
            return endpoint

        except Exception as e:
            raise e
        
    def receive_chunk(self, chunk, endpoint):
        '''
        Recebe um chunk de um arquivo.
        :param chunk: chunk a ser recebido
        :param index: indice do arquivo
        '''
        try:
            worker = self.workers[endpoint[1]]
            eof = worker.write_chunk(chunk, endpoint[0])
            return eof
        except Exception as e:
            raise e
        
    def cp_to(self, path, user):
        '''
        Copia um arquivo do worker para o cliente
        :param path: caminho do arquivo de origem
        '''
        try:
            # verifica se o caminho esta no formato correto
            path = Path(path)
            if not path.is_absolute():
                path = Path(f"/NFS/{user}") / path

            p_string = str(path)

            # verifica se o arquivo existe
            if p_string not in self.files.keys():
                raise Exception("Arquivo nao encontrado")

            hash = self.files[p_string]
            worker_name = self.files_map[hash]
            worker = self.workers[worker_name]
            worker._pyroClaimOwnership()
            index = worker.open_file(hash, "rb")
            endpoint = (index, worker_name) # indice do arquivo aberto no worker e o nome do worker
            return endpoint

        except Exception as e:
            raise e
        
    def send_chunk(self, endpoint):
        '''
        Envia um chunk de um arquivo.
        :param index: indice do arquivo
        '''
        try:
            worker = self.workers[endpoint[1]]
            for chunk in worker.read_chunks(endpoint[0]):
                yield chunk
        except Exception as e:
            raise e
        
    def rm(self, paths, user):
        '''
        Remove um arquivo do servidor mestre.
        :param path: caminho do arquivo a ser removido
        '''
        try:
            for path in paths:
                path = Path(path)
                if not path.is_absolute():
                    path = Path(f"/NFS/{user}") / path

                p_string = str(path)

                # verifica se o arquivo existe
                if p_string not in self.files.keys():
                    raise Exception("Arquivo nao encontrado")

                hash = self.files[p_string]
                worker_name = self.files_map[hash]
                self.workers[worker_name].rm(hash)
                del self.files[p_string]
                del self.files_map[hash]
                self.save_file()

                return True
        
        except Exception as e:
            raise e
        
    def save_file(self):
        '''
        Salva o arquivo de arquivos do servidor mestre.
        '''
        try:
            with open("files.txt", "w") as f:
                for key, value in self.files.items():
                    f.write(f"{key}:{value}\n")

            with open("files_map.txt", "w") as f:
                for key, value in self.files_map.items():
                    f.write(f"{key}:{value}\n")

        except Exception as e:
            print(f"Erro ao salvar o arquivo de arquivos: {e}")
        
def main():
    daemon = Pyro5.server.Daemon()         # make a Pyro daemon
    ns = Pyro5.api.locate_ns()       # find the name server
    server = ServerMaster()             
    uri = daemon.register(server)   # register the greeting maker as a Pyro object
    ns.register("fs_server", uri)   # register the object with a name in the name server

    print("Ready.")
    daemon.requestLoop() 

if __name__ == "__main__":
    main()