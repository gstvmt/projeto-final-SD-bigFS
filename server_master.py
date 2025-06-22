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
import base64
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

        self.block_size = 16 * 1024 * 1024  # 16MB por bloco
        self.num_replic = 2                 # Número de réplicas por bloco
        self.rr_counter = 0  # Contador para round-robin

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

    def get_round_robin_worker(self):
        """
            Seleciona um worker disponível usando o algoritmo round-robin.

            -------------------------------------------------------
            Funcionamento geral:
                Esta função implementa um algoritmo round-robin para selecionar workers
                de forma balanceada, garantindo que apenas workers ativos (status 'on')
                sejam retornados.

            -------------------------------------------------------
            Principais tarefas da função:

                1. Verifica se existem workers registrados.
                
                2. Percorre a lista de workers em ordem round-robin.
                
                3. Verifica o status do worker através do last_heartbeats.
                
                4. Retorna o primeiro worker ativo encontrado.

            -------------------------------------------------------
            Variáveis e estruturas importantes:

            - `self.workers`: Dicionário contendo os workers disponíveis.
            - `self.rr_counter`: Contador interno para o algoritmo round-robin.
            - `self.last_heartbeats`: Dicionário com status dos workers.
                Formato:
                    {
                        'worker1': {'status': 'on'/'off', ...},
                        ...
                    }

            -------------------------------------------------------
            Parâmetros:
                Nenhum (usa apenas atributos da classe)

            -------------------------------------------------------
            Retorno:
                (str) Nome do worker selecionado

        """
        if not self.workers:
            raise Exception("No workers available")
        
        # Pega o próximo worker na sequência
        workers = list(self.workers.keys())
        while True:
            worker_name = workers[self.rr_counter % len(workers)]
            self.rr_counter = (self.rr_counter + 1) % len(workers)
            if self.last_heartbeats.get(worker_name, {}).get('status') == 'on':
                break
    
        return worker_name

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
            Inicia o processo de cópia de um arquivo do cliente para o sistema distribuído.

            -------------------------------------------------------
            Funcionamento geral:
                Esta função prepara a estrutura para receber um novo arquivo ou continuar o upload de um arquivo existente.
                Cria as entradas necessárias nos dicionários de controle e retorna um endpoint para upload em blocos.

            -------------------------------------------------------
            Principais tarefas da função:

                1. Normaliza o caminho do arquivo, adicionando o prefixo /NFS/{user} caso o path não seja absoluto.
                
                2. Verifica se o arquivo já existe no sistema. Caso não exista, inicializa sua estrutura de dados.
                
                3. Retorna um dicionário com informações para o upload em blocos, incluindo:
                    - Caminho completo do arquivo
                    - Próximo bloco a ser recebido
                    - Tamanho atual do arquivo

            -------------------------------------------------------
            Variáveis e estruturas:

            - `self.files`: Dicionário que armazena metadados sobre os arquivos.
                Formato:
                    self.files = {
                        '<caminho_arquivo>': {
                            'blocks': [<hash_bloco1>, <hash_bloco2>, ...],
                            'size': <tamanho_total_arquivo>
                        },
                        ...
                    }
            
            - `path`: Caminho do arquivo fornecido pelo usuário.
            - `user`: Nome do usuário solicitante (para namespace isolado no NFS).

            -------------------------------------------------------
            Parâmetros:
                :param path: (str) Caminho relativo ou absoluto do arquivo no sistema de arquivos.
                :param user: (str) Identificação do usuário dono do arquivo.

            -------------------------------------------------------
            Retorno:
                (dict) Endpoint para upload contendo:
                    {
                        'file_path': (str) caminho completo do arquivo,
                        'next_block': (int) próximo bloco a ser recebido (inicia em 0),
                        'current_size': (int) tamanho atual do arquivo (inicia em 0)
                    }

        '''

        try:
            path = Path(path)
            if not path.is_absolute():
                path = Path(f"/NFS/{user}") / path

            p_string = str(path)

            # Se o arquivo já existe, retorna erro
            if p_string in self.files.keys():
                if self.files[p_string]['size'] > 0:
                    raise Exception(f"Arquivo {p_string} já existe. Use outro nome ou delete o arquivo existente.")
            
            # Inicializa a estrutura do arquivo
            self.files[p_string] = {
                'blocks': [],
                'size': 0
            }
            self.save_file()
                
            # Retorna um endpoint especial para upload em blocos
            return {
                'file_path': p_string,
                'next_block': 0,
                'current_size': 0
            }

        except Exception as e:
            raise e
        
    def receive_chunk(self, chunk_data, endpoint_info):
        '''
            Processa um chunk de dados recebido e o armazena nos nós.

            -------------------------------------------------------
            Funcionamento geral:
                Esta função recebe um pedaço (chunk) de um arquivo e o armazena nos workers conforme
                a política de replicação definida. Gerencia todo o processo de escrita distribuída.

            -------------------------------------------------------
            Principais tarefas da função:

                1. Decodifica o chunk de dados (caso esteja em base64).
                
                2. Para novos blocos (current_size == 0):
                    a. Calcula o hash de identificação do bloco
                    b. Seleciona os workers para replicação usando round-robin
                    c. Armazena o mapeamento bloco→workers
                
                3. Escreve o chunk em todos os workers de replicação:
                    a. Abre o arquivo no worker (se for primeiro chunk do bloco)
                    b. Escreve o chunk
                    c. Fecha o arquivo (quando bloco é completado)
                
                4. Atualiza os metadados do arquivo (tamanho total, blocos, etc).

            -------------------------------------------------------
            Variáveis e estruturas importantes:

            - `self.files_map`: Dicionário que mapeia blocos para workers de replicação.
                Formato:
                    self.files_map = {
                        '<hash_do_bloco>': ['worker1', 'worker2', ...],
                        ...
                    }
            
            - `self.block_size`: Tamanho máximo de cada bloco (em bytes).
            - `self.num_replic`: Número de réplicas para cada bloco.
            - `self.workers`: Dicionário de workers disponíveis.
            - `endpoint_info`: Estado atual do upload contendo:
                {
                    'file_path': (str) caminho do arquivo,
                    'next_block': (int) próximo bloco esperado,
                    'current_size': (int) bytes recebidos no bloco atual,
                    '<worker_name>': (int) handle do arquivo aberto no worker (opcional)
                }

            -------------------------------------------------------
            Parâmetros:
                :param chunk_data: (bytes/dict) Dados do chunk, pode ser raw bytes ou dict com {'data': b64, 'encoding': 'base64'}
                :param endpoint_info: (dict) Estado atual do upload (retornado por cp_from ou chamada anterior)

            -------------------------------------------------------
            Retorno:
                (tuple) Contendo:
                    - eof: (bool) Indica se foi o último chunk do arquivo
                    - endpoint_info: (dict) Estado atualizado do upload

        '''
        try:
            if isinstance(chunk_data, dict) and 'data' in chunk_data and chunk_data.get('encoding') == 'base64':
                chunk = base64.b64decode(chunk_data['data'])
            else:
                chunk = chunk_data

            p_string = endpoint_info['file_path']
            block_num = endpoint_info['next_block']
            current_size = endpoint_info['current_size']
            eof = False

            
            # Gera o hash do bloco atual (mesmo que não seja novo)
            block_identifier = f"{p_string}_block{block_num}"
            block_hash = hashlib.sha256(block_identifier.encode()).hexdigest()

            # Se for o primeiro chunk de um novo bloco
            if current_size == 0:
                temp_hash = block_hash
                replicas = []

                for _ in range(self.num_replic):
                    node = self.get_round_robin_worker() #self.hash_ring.get_node(temp_hash)
                    print(f"Selecionando nó: {node}")
                    replicas.append(node)
                    # Roda o hash novamente para selecionar diferentes nós
                    temp_hash = hashlib.sha256(temp_hash.encode()).hexdigest()

                # Armazena o mapeamento apenas se for um novo bloco
                if block_hash not in self.files_map:
                    self.files_map[block_hash] = replicas
                    self.files[p_string]['blocks'].append(block_hash)


            # Envia o chunk para todos os workers de réplica
            for worker_name in self.files_map[block_hash]:
                worker = self.workers[worker_name]
                worker._pyroClaimOwnership()
                
                # Se for o primeiro chunk, abre o arquivo
                if current_size == 0:
                    index = worker.open_file(block_hash, "wb")
                    endpoint_info[worker_name] = index

                # Escreve o chunk
                eof = worker.write_chunk(chunk, endpoint_info[worker_name])
            
            # Atualiza o endpoint_info
            endpoint_info['current_size'] += len(chunk)

            # Se completou o bloco
            if endpoint_info['current_size'] >= self.block_size:
                print("Bloco completo, atualizando informações...")
                endpoint_info['next_block'] += 1
                endpoint_info['current_size'] = 0
                # Fecha os arquivos nos workers
                print(f"self.files_map[block_hash]: {self.files_map[block_hash]}")
                for worker_name in self.files_map[block_hash]:
                    worker = self.workers[worker_name]
                    worker.close_block(endpoint_info[worker_name])
                    del endpoint_info[worker_name]


            # Atualiza o tamanho total do arquivo
            self.files[p_string]['size'] += len(chunk)
            self.save_file()
            
            return eof, endpoint_info

        except Exception as e:
            raise e
        
    def cp_to(self, path, user):
        '''
            Prepara o download para o cliente.

            -------------------------------------------------------
            Funcionamento geral:
                Esta função verifica a existência do arquivo solicitado e cria uma estrutura
                de endpoint para gerenciar o download em blocos do arquivo distribuído.

            -------------------------------------------------------
            Principais tarefas da função:

                1. Normaliza o caminho do arquivo, adicionando o prefixo /NFS/{user} caso o path não seja absoluto.
                
                2. Verifica se o arquivo existe no sistema através do dicionário self.files.
                
                3. Cria e retorna uma estrutura de endpoint contendo todas as informações necessárias
                para realizar o download em blocos do arquivo.

            -------------------------------------------------------
            Variáveis e estruturas importantes:

            - `self.files`: Dicionário que armazena metadados sobre os arquivos.
                Formato:
                    self.files = {
                        '<caminho_arquivo>': {
                            'blocks': [<hash_bloco1>, <hash_bloco2>, ...],
                            'size': <tamanho_total_arquivo>
                        },
                        ...
                    }
            
            - `path`: Caminho do arquivo fornecido pelo usuário.
            - `user`: Nome do usuário solicitante (para namespace isolado no NFS).

            -------------------------------------------------------
            Parâmetros:
                :param path: (str) Caminho relativo ou absoluto do arquivo no sistema de arquivos.
                :param user: (str) Identificação do usuário dono do arquivo.

            -------------------------------------------------------
            Retorno:
                (dict) Endpoint para download contendo:
                    {
                        'file_path': (str) caminho completo do arquivo,
                        'current_block': (int) índice do bloco atual (inicia em 0),
                        'block_offset': (int) offset dentro do bloco atual (inicia em 0),
                        'total_size': (int) tamanho total do arquivo em bytes,
                        'blocks': (list) lista de hashes dos blocos do arquivo,
                        'workers': (dict) vazio inicialmente, armazenará handles de workers
                    }

        '''
        try:
            # Normaliza o caminho
            path = Path(path)
            if not path.is_absolute():
                path = Path(f"/NFS/{user}") / path

            p_string = str(path)

            # Verifica se o arquivo existe
            if p_string not in self.files:
                raise Exception("Arquivo não encontrado")

            # Cria estrutura de endpoint para download em blocos
            endpoint = {
                'file_path': p_string,
                'current_block': 0,
                'block_offset': 0,
                'total_size': self.files[p_string]['size'],
                'blocks': self.files[p_string]['blocks'],
                'workers': {}  # Armazenará os workers ativos para cada bloco
            }

            return endpoint

        except Exception as e:
            raise e
        
    def send_chunk(self, endpoint):
        '''
            Envia chunks de um nó para o cliente.

            -------------------------------------------------------
            Funcionamento geral:
                Esta função implementa um gerador que percorre todos os blocos do arquivo,
                lendo cada bloco de um worker disponível e enviando os chunks sequencialmente.

            -------------------------------------------------------
            Principais tarefas da função:

                1. Para cada bloco do arquivo (em ordem):
                    a. Encontra um worker disponível que possua o bloco
                    b. Abre o arquivo no worker em modo leitura
                    c. Lê e envia os chunks sequencialmente
                    d. Fecha o arquivo no worker após ler todo o bloco
                
                2. Mantém o controle do progresso do download através do endpoint.

            -------------------------------------------------------
            Variáveis e estruturas importantes:

            - `self.files_map`: Dicionário que mapeia blocos para workers de replicação.
                Formato:
                    self.files_map = {
                        '<hash_do_bloco>': ['worker1', 'worker2', ...],
                        ...
                    }
            
            - `self.block_size`: Tamanho máximo de cada bloco (em bytes).
            - `self.workers`: Dicionário de workers disponíveis.
            - `endpoint`: Estado atual do download contendo:
                {
                    'file_path': (str) caminho do arquivo,
                    'current_block': (int) índice do bloco atual,
                    'block_offset': (int) offset dentro do bloco atual,
                    'total_size': (int) tamanho total do arquivo,
                    'blocks': (list) lista de hashes dos blocos,
                    'workers': (dict) mapeamento bloco→(worker_name, handle)
                }

            -------------------------------------------------------
            Parâmetros:
                :param endpoint: (dict) Estado do download (retornado por cp_to)

            -------------------------------------------------------
            Retorno:
                (generator) Que produz:
                    (dict) Chunks de dados no formato:
                        {
                            'data': (str) dados codificados em base64,
                            'encoding': 'base64'
                        }

        '''
        try:
            # Percorre todos os blocos do arquivo
            while endpoint['current_block'] < len(endpoint['blocks']):
                block_hash = endpoint['blocks'][endpoint['current_block']]
                
                # Se não temos um worker aberto para este bloco
                if block_hash not in endpoint['workers']:
                    # Pega a lista de workers que possuem este bloco
                    available_workers = self.files_map.get(block_hash, [])
                    
                    if not available_workers:
                        raise Exception(f"Nenhum worker disponível para o bloco {block_hash}")
                    
                    # Tenta encontrar um worker disponível
                    for worker_name in available_workers:
                        try:
                            worker = self.workers[worker_name]
                            worker._pyroClaimOwnership()
                            index = worker.open_file(block_hash, "rb")
                            endpoint['workers'][block_hash] = (worker_name, index)
                            break
                        except:
                            continue
                    else:
                        raise Exception(f"Não foi possível acessar o bloco {block_hash} em nenhum worker")
                
                worker_name, index = endpoint['workers'][block_hash]
                worker = self.workers[worker_name]
                
                # Usa o read_chunks existente do worker
                for chunk in worker.read_chunks(index):
                    yield chunk
                    
                    # Atualiza o progresso (opcional)
                    endpoint['block_offset'] += len(base64.b64decode(chunk['data']))
                    
                    # Verifica se terminou o bloco (baseado no tamanho esperado)
                    if endpoint['block_offset'] >= self.block_size:
                        endpoint['block_offset'] = 0
                        break
                
                # Move para o próximo bloco
                endpoint['current_block'] += 1
                
            # Limpeza final
            for block_hash, (worker_name, index) in endpoint['workers'].items():
                try:
                    # O worker já fecha o arquivo automaticamente no read_chunks
                    # quando termina de ler, mas fazemos uma verificação adicional
                    if index in self.workers[worker_name].opened_files:
                        self.workers[worker_name].close_block(index)
                except:
                    pass

        except Exception as e:
            # Limpeza em caso de erro
            for block_hash, (worker_name, index) in endpoint['workers'].items():
                try:
                    if index in self.workers[worker_name].opened_files:
                        self.workers[worker_name].close_block(index)
                except:
                    pass
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