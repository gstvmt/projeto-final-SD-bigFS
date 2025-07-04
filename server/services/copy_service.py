# services/copy_service.py

from concurrent.futures import ThreadPoolExecutor
import threading
import Pyro5.api
import base64
import random
import queue
import time
import uuid

# --- Constantes de Configuração --
MAX_RETRIES = 5
RETRY_DELAY = 2

# CLASSE DE SESSÃO PARA UPLOAD
@Pyro5.api.expose
class UploadSession:
    """
        Gerencia um único upload. Obtém a lista de DataNodes ativos do registro
        e se comunica diretamente com eles.
    """
    def __init__(self, daemon, metadata_repo, datanode_registry, dfs_path, replica_count=2):
        """
            Inicializa uma nova sessão de upload.

            -------------------------------------------------------
            Parâmetros:
                daemon : Pyro5.server.Daemon
                    Instância do daemon Pyro para comunicação RMI
                metadata_repo : Objeto MetadataRepository
                datanode_registry : DObjeto ataNodeRegistry
                dfs_path : str
                    Caminho destino no sistema de arquivos distribuído
                replica_count : int, opcional (padrão=2)
                    Fator de replicação para cada bloco

            -------------------------------------------------------
            Configurações:
                - block_size: 32MB (tamanho fixo dos blocos)
                - max_workers: 10 (threads para upload paralelo)

            -------------------------------------------------------
            Inicialização:
                1. Gera ID único para a sessão
                2. Configura parâmetros de replicação
                3. Prepara estruturas para armazenar metadados
                4. Inicializa thread pool para operações paralelas
        """
        self._daemon = daemon
        self._metadata_repo = metadata_repo
        self._datanode_registry = datanode_registry
        self._dfs_path = dfs_path
        self._replica_count = replica_count
        self.block_size = 32 * 1024 * 1024  # 32 MB
        
        self.session_id = str(uuid.uuid4())
        self.block_metadata = []
        self.is_active = True
        self.executor = ThreadPoolExecutor(max_workers=10)
        print(f"Sessão de Upload {self.session_id} criada para '{dfs_path}'.")

    def _write_block_to_node(self, uri, block_id, data):
        """
            Executa a escrita de um bloco em um DataNode específico via RMI.

            -------------------------------------------------------
            Funcionamento geral:
                Estabelece conexão com o DataNode através de Pyro5 e executa
                a operação de escrita remota do bloco.

            -------------------------------------------------------
            Parâmetros:
                uri : str
                    Endereço URI do DataNode alvo
                block_id : str
                    Identificador único do bloco
                data : bytes
                    Dados binários a serem escritos

            -------------------------------------------------------
            Retorno:
                bool
                    True se a escrita foi bem-sucedida, False caso contrário

        """
        
        try:
            with Pyro5.api.Proxy(uri) as proxy:
                return proxy.write_block(block_id, data)
        except Exception as e:
            print(f"ERRO ao escrever bloco {block_id} em {uri}: {e}")
            return False

    def write_chunk(self, chunk_data, endpoint_info):
        """
            Processa um chunk de dados e o escreve nos DataNodes com replicação.

            -------------------------------------------------------
            Funcionamento geral:
                1. Decodifica os dados se estiverem em base64
                2. Seleciona nós para replicação
                3. Executa escrita distribuída com tolerância a falhas
                4. Atualiza metadados do bloco quando completo

            -------------------------------------------------------
            Parâmetros:
                chunk_data : dict ou bytes
                    Dados a serem escritos (pode ser dict com encoding base64)
                endpoint_info : dict
                    Estado atual do upload contendo:
                    - file_path: caminho do arquivo
                    - next_block: número do próximo bloco
                    - current_size: tamanho acumulado no bloco atual
                    - nodes: lista de nós usados para replicação

            -------------------------------------------------------
            Retorno:
                dict
                    endpoint_info atualizado após processamento do chunk

        """
        
        if not self.is_active:
            raise RuntimeError("Sessão de upload não está mais ativa.")

        if isinstance(chunk_data, dict) and 'data' in chunk_data and chunk_data.get('encoding') == 'base64':
            chunk = base64.b64decode(chunk_data['data'])
        else:
            chunk = chunk_data

        len_chunk = len(chunk)

        p_string = endpoint_info['file_path']
        block_num = endpoint_info['next_block']
        current_size = endpoint_info['current_size']
        nodes = endpoint_info.get('nodes', [])
        
        active_nodes = self._datanode_registry.get_available_nodes()
        if len(active_nodes) < self._replica_count:
            raise IOError(f"Nós de dados ativos ({len(active_nodes)}) insuficientes para o fator de replicação ({self._replica_count}).")
        
        # Escolhe os nós para as réplicas de forma aleatória
        nodes_for_replicas = []
        if current_size == 0:
            nodes_for_replicas = random.sample(active_nodes, k=self._replica_count)
            endpoint_info["nodes"] = nodes_for_replicas
        else:
            # Continua o upload, reutilizando os nós já escolhidos
            nodes_for_replicas = endpoint_info['nodes']
        block_id = f"{p_string}_block{block_num}"

        attempt = 0
        while attempt < MAX_RETRIES:
            attempt += 1

            # Submete as tarefas de escrita para o pool de threads
            futures = [
                self.executor.submit(self._write_block_to_node, uri, block_id, chunk)
                for uri in nodes_for_replicas
            ]
            results = [future.result() for future in futures]

            if all(results):
                break 
            else:
                print(f"Falha ao escrever bloco {block_id} em uma ou mais réplicas.")
                if attempt < MAX_RETRIES:
                    try:
                        for uri in nodes:
                            print(f"Fechando bloco {block_id} no nó {uri}...")
                            with Pyro5.api.Proxy(uri) as proxy:
                                proxy.close_block(block_id)
                                print(f"Bloco {block_id} fechado no nó {uri}.")
                    except Exception as e:
                        print(f"Erro ao fechar blocos nos nós: {e}")

                    endpoint_info['current_size'] = 0
                    active_nodes = self._datanode_registry.get_available_nodes()
                    nodes_for_replicas = random.sample(active_nodes, k=self._replica_count)
                    endpoint_info["nodes"] = nodes_for_replicas
                    print(f"Aguardando {RETRY_DELAY} segundos antes de tentar novamente...")
                    time.sleep(RETRY_DELAY)
                else:
                    print(f"Tentativas esgotadas para o bloco {block_id}.")
                    raise IOError("Falha ao escrever uma ou mais réplicas do bloco após múltiplas tentativas.")



        endpoint_info['current_size'] += len_chunk
        # Se completou o bloco
        if endpoint_info['current_size'] >= self.block_size:
            print("Bloco completo, atualizando informações...")
            endpoint_info['next_block'] += 1
            endpoint_info['current_size'] = 0
            endpoint_info['nodes'] = []
            
            # Fecha os arquivos nos datanodes
            for uri in nodes:
                with Pyro5.api.Proxy(uri) as proxy:
                    proxy.close_block(block_id)

            block_info = {
                "block_order": len(self.block_metadata),
                "block_id": block_id,
                "replicas": nodes_for_replicas
            }

            self.block_metadata.append(block_info)

        return endpoint_info

    def close(self, endpoint_info):
        """
            Finaliza o processamento dos blocos remanescentes de um upload.

            -------------------------------------------------------
            Funcionamento geral:
                Processa qualquer dado remanescente no buffer de upload que não preencheu
                completamente um bloco, fechando os arquivos abertos nos DataNodes e
                registrando os metadados finais.

            -------------------------------------------------------
            Parâmetros:
                endpoint_info : dict
                    Estado atual do upload contendo:
                    - file_path: caminho lógico do arquivo
                    - next_block: número do próximo bloco
                    - current_size: tamanho acumulado no bloco atual
                    - nodes: lista de nós usados para replicação

            -------------------------------------------------------
            Comportamento:
                - Se houver dados não commitados (current_size > 0):
                    1. Gera o block_id final
                    2. Fecha os arquivos nos DataNodes
                    3. Registra os metadados do bloco final
                - Não há retorno explícito (operações são side-effects)

        """

        p_string = endpoint_info['file_path']
        block_num = endpoint_info['next_block']
        current_size = endpoint_info['current_size']
        nodes = endpoint_info.get('nodes', [])

        if current_size > 0:
            block_id = f"{p_string}_block{block_num}"

            # Fecha os blocos restantes nos nós
            for uri in nodes:
                with Pyro5.api.Proxy(uri) as proxy:
                    proxy.close_block(block_id)
                
            block_info = {
                "block_order": len(self.block_metadata),
                "block_id": block_id,
                "replicas": nodes
            }
            self.block_metadata.append(block_info)

        
    def commit(self, total_size):
        """
            Finaliza a sessão de upload registrando os metadados no sistema.

            -------------------------------------------------------
            Funcionamento geral:
                1. Valida se a sessão ainda está ativa
                2. Cria a estrutura de metadados do arquivo
                3. Persiste os metadados no repositório
                4. Executa cleanup da sessão

            -------------------------------------------------------
            Parâmetros:
                total_size : int
                    Tamanho total do arquivo em bytes

            -------------------------------------------------------
            Retorno:
                dict
                    Resultado da operação com formato:
                    {
                        "status": "success"|"error",
                        "message": string descritiva
                    }

            -------------------------------------------------------
            Estrutura de metadados:
                {
                    "type": "file",
                    "size": total_size,
                    "blocks": [
                        {
                            "block_order": int,
                            "block_id": string,
                            "replicas": [lista_de_uris]
                        },
                        ...
                    ]
                }
        """
        if not self.is_active:
            return {"status": "error", "message": "Sessão já finalizada."}
        
        print(f"[{self.session_id}] Finalizando upload para '{self._dfs_path}'.")
        
        file_metadata = {
            "type": "file",
            "size": total_size,
            "blocks": self.block_metadata
        }

        success = self._metadata_repo.add_entry(self._dfs_path, file_metadata)
        if not success:
            # Lógica de rollback (deletar blocos já escritos) seria necessária aqui.
            raise IOError("Falha ao commitar metadados.")

        print(f"[{self.session_id}] Commit bem-sucedido. Encerrando sessão.")
        self.cleanup()
        return {"status": "success", "message": f"Arquivo '{self._dfs_path}' criado com sucesso."}

    @Pyro5.api.oneway
    def abort(self):
        print(f"[{self.session_id}] Abortando upload.")
        self.cleanup()

    def cleanup(self):
        self.is_active = False
        self.executor.shutdown(wait=False)
        self._daemon.unregister(self)
        print(f"Sessão {self.session_id} limpa e desregistrada.")


# CLASSE DE SESSÃO PARA DOWNLOAD
@Pyro5.api.expose
class DownloadSession:
    def __init__(self, daemon, block_list, datanode_registry):
        """
            Inicializa uma nova sessão de download.

            -------------------------------------------------------
            Parâmetros:
                daemon : Pyro5.server.Daemon
                    Instância do daemon Pyro para chamadas RMI

                block_list : list
                    Lista de blocos do arquivo (com metadados)

                datanode_registry : DataNodeRegistry
                    Registro de nós de dados disponíveis

            -------------------------------------------------------
            Comportamento:
                1. Ordena blocos por ordem numérica
                2. Inicia thread de pré-busca (prefetch)
                3. Configura buffer com capacidade para 10 blocos
        """

        self._daemon = daemon
        self._block_list = sorted(block_list, key=lambda b: b['block_order'])
        self._datanode_registry = datanode_registry
        self.session_id = str(uuid.uuid4())
        self.is_active = True
        self.buffer = queue.Queue(maxsize=10)

        self.prefetch_thread = threading.Thread(target=self._prefetch_blocks)
        self.prefetch_thread.daemon = True
        self.prefetch_thread.start()
        print(f"Sessão de Download {self.session_id} criada.")

    def _read_block_from_node(self, uri, block_id):
        """
            Lê um bloco de dados de um DataNode específico.

            -------------------------------------------------------
            Parâmetros:
                uri : str
                    Endereço do DataNode
                block_id : str
                    Identificador do bloco

            -------------------------------------------------------
            Retorno:
                bytes
                    Conteúdo do bloco ou exceção em caso de falha
        """
        try:
            with Pyro5.api.Proxy(uri) as proxy:
                return proxy.read_block(block_id)
        except Exception as e:
            print(f"ERRO ao ler bloco {block_id} de {uri}: {e}")
            # Propaga a exceção para ser colocada no buffer
            raise

    def _choose_active_replica(self, replicas, active_nodes):
        """
            Seleciona uma réplica ativa para leitura.

            -------------------------------------------------------
            Parâmetros:
                replicas : list
                    Lista de URIs de réplicas disponíveis
                active_nodes : list
                    Lista de nós ativos no registry

            -------------------------------------------------------
            Retorno:
                str ou None
                    URI da réplica selecionada ou None se nenhuma disponível
        """
        active_replicas = [replica for replica in replicas if replica in active_nodes]
        if not active_replicas:
            return None  # Nenhuma réplica ativa
        return random.choice(active_replicas)
    
    def _prefetch_blocks(self):
        """
            Executa pré-busca paralela dos blocos do arquivo.

            -------------------------------------------------------
            Funcionamento:
                1. Cria pool de threads para leitura paralela
                2. Para cada bloco:
                    - Seleciona réplica ativa
                    - Submete tarefa de leitura
                3. Coloca resultados no buffer:
                    - Dados dos blocos
                    - Exceções em caso de falha
                    - None ao finalizar

        """
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_reads = {}
            for block in self._block_list:
                active_nodes = self._datanode_registry.get_available_nodes()
                chosen_replica = self._choose_active_replica(block['replicas'], active_nodes)
                if chosen_replica:
                    future = executor.submit(self._read_block_from_node, chosen_replica, block['block_id'])
                    future_reads[future] = block
                else:
                    # se nenhuma réplica estiver disponível, comocamos uma exceção no buffer para enviar ao cliente
                    self.buffer.put(Exception(f"Nenhuma réplica ativa disponível para o bloco {block['block_id']}"))


            for future in future_reads:
                try:
                    block_data = future.result()
                    self.buffer.put(block_data)
                except Exception as e:
                    self.buffer.put(e) # insere a exceção no buffer
                    return
        self.buffer.put(None)

    def read_chunk(self):
        if not self.is_active: return None
        chunk = self.buffer.get()
        if isinstance(chunk, Exception): self.cleanup(); raise chunk
        if chunk is None: self.cleanup(); return None
        return chunk

    def cleanup(self):
        if self.is_active:
            self.is_active = False
            self._daemon.unregister(self)
            print(f"Sessão de Download {self.session_id} limpa.")

# CLASSE DE SERVIÇO PRINCIPAL
@Pyro5.api.expose
class CopyService:
    def __init__(self, daemon, metadata_repo, datanode_registry):
        """
            Inicializa o serviço de cópia.

            -------------------------------------------------------
            Parâmetros:
                daemon : Pyro5.server.Daemon
                    Instância do daemon Pyro para registro de objetos
                metadata_repo : Objeto MetadataRepository
                datanode_registry : Objeto DataNodeRegistry

        """
        self._daemon = daemon
        self._metadata_repo = metadata_repo
        self._datanode_registry = datanode_registry

        print("CopyService inicializado.")

    def initiate_upload(self, dfs_path, client_name):
        """
            Inicia uma nova sessão de upload.

            -------------------------------------------------------
            Parâmetros:
                dfs_path : str
                    Caminho desejado no sistema de arquivos distribuído
                client_name : str
                    Identificador do cliente (para namespacing)

            -------------------------------------------------------
            Retorno:
                dict
                    Contendo:
                    - session_uri: URI da sessão criada
                    - endpoint: Informações iniciais para upload

            -------------------------------------------------------
            Comportamento:
                1. Cria caminho único combinando client_name e dfs_path
                2. Instancia nova UploadSession
                3. Registra sessão no daemon Pyro
                4. Retorna URI e endpoint inicial
        """

        # juntando os nomes para criar um caminho único
        clean_path = dfs_path.removeprefix("dfs:")
        dfs_path = "".join([client_name, clean_path]) if client_name else clean_path

        print(f"Iniciando uma nova sessão de upload para: {dfs_path}")

        # inicializa a sessao de upload e os metadados do bloco
        session = UploadSession(self._daemon, self._metadata_repo, self._datanode_registry, dfs_path)
        session_uri = self._daemon.register(session)
        endpoint = {
                'file_path': dfs_path,
                'next_block': 0,
                'current_size': 0,
                'nodes': []
            }
        
        return {"session_uri": str(session_uri), "endpoint": endpoint}
    
    def initiate_download(self, dfs_path, client_name):
        """
            Inicia uma nova sessão de download.

            -------------------------------------------------------
            Parâmetros:
                dfs_path : str
                    Caminho do arquivo no sistema distribuído
                client_name : str
                    Identificador do cliente (para logging)

            -------------------------------------------------------
            Retorno:
                dict
                    Contendo:
                    - session_uri: URI da sessão criada
                    - message: Mensagem adicional

            -------------------------------------------------------
            Comportamento:
                1. Verifica existência do arquivo nos metadados
                2. Cria DownloadSession se arquivo válido
                3. Registra sessão no daemon Pyro
                4. Retorna URI da sessão
        """

        clean_path = dfs_path.removeprefix("dfs:")
        dfs_path = "".join([client_name, clean_path]) if client_name else clean_path

        print(f"[CopyService] Iniciando sessão de download para: {dfs_path}")
        entry_info = self._metadata_repo.get_entry(dfs_path)
        if not entry_info or entry_info.get("type") != "file":
            raise FileNotFoundError(f"Arquivo não encontrado: {dfs_path}")
        
        block_list = entry_info.get("blocks", [])
        if not block_list:
            return {"session_uri": None, "message": "Arquivo vazio."}

        session = DownloadSession(self._daemon, block_list, self._datanode_registry)
        session_uri = self._daemon.register(session)
        return {"session_uri": str(session_uri)}