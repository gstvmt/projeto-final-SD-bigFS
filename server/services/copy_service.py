# services/copy_service.py

import time
import base64
import Pyro5.api
import threading
import uuid
import queue
import random
from concurrent.futures import ThreadPoolExecutor

# ==============================================================================
# CLASSE DE SESSÃO PARA UPLOAD
# ==============================================================================

MAX_RETRIES = 5
RETRY_DELAY = 2

@Pyro5.api.expose
class UploadSession:
    """
    Gerencia um único upload. Obtém a lista de DataNodes ativos do registro
    e se comunica diretamente com eles.
    """
    def __init__(self, daemon, metadata_repo, datanode_registry, dfs_path, replica_count=2):
        self._daemon = daemon
        self._metadata_repo = metadata_repo
        # CORREÇÃO: Recebe o registro de nós, não um cliente
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
        """Função auxiliar que faz a chamada RMI real para um DataNode."""
        try:
            with Pyro5.api.Proxy(uri) as proxy:
                return proxy.write_block(block_id, data)
        except Exception as e:
            print(f"ERRO ao escrever bloco {block_id} em {uri}: {e}")
            return False

    def write_chunk(self, chunk_data, endpoint_info):
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
        
        # CORREÇÃO: Lógica de seleção de nós, sem simulação.
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
            
            # Fecha os arquivos nos workers
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
        Método chamado pelo cliente para fechar os blocos restantes.
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

# ==============================================================================
# CLASSE DE SESSÃO PARA DOWNLOAD
# ==============================================================================
@Pyro5.api.expose
class DownloadSession:
    def __init__(self, daemon, block_list, datanode_registry):
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
        """Função auxiliar que faz a chamada RMI real para um DataNode."""
        try:
            with Pyro5.api.Proxy(uri) as proxy:
                return proxy.read_block(block_id)
        except Exception as e:
            print(f"ERRO ao ler bloco {block_id} de {uri}: {e}")
            # Propaga a exceção para ser colocada no buffer
            raise

    def _choose_active_replica(self, replicas, active_nodes):
        active_replicas = [replica for replica in replicas if replica in active_nodes]
        if not active_replicas:
            return None  # Nenhuma réplica ativa
        return random.choice(active_replicas)
    
    def _prefetch_blocks(self):
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_reads = {}
            for block in self._block_list:
                active_nodes = self._datanode_registry.get_available_nodes()
                chosen_replica = self._choose_active_replica(block['replicas'], active_nodes)
                if chosen_replica:
                    future = executor.submit(self._read_block_from_node, chosen_replica, block['block_id'])
                    future_reads[future] = block
                else:
                    # Se nenhuma réplica está disponível, já coloca a exceção no buffer para o cliente saber
                    self.buffer.put(Exception(f"Nenhuma réplica ativa disponível para o bloco {block['block_id']}"))


            for future in future_reads:
                try:
                    block_data = future.result()
                    self.buffer.put(block_data)
                except Exception as e:
                    self.buffer.put(e) # Coloca a exceção no buffer para o cliente saber
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

# ==============================================================================
# CLASSE DE SERVIÇO PRINCIPAL (A FÁBRICA DE SESSÕES)
# ==============================================================================
@Pyro5.api.expose
class CopyService:
    def __init__(self, daemon, metadata_repo, datanode_registry):
        self._daemon = daemon
        self._metadata_repo = metadata_repo
        # CORREÇÃO: Armazena o registro de nós para passar para as sessões
        self._datanode_registry = datanode_registry
        print("CopyService (Fábrica de Sessões) inicializado.")

    def initiate_upload(self, dfs_path, client_name):
        # Juntando os nomes para criar um caminho único com join
        dfs_path = "".join([client_name, dfs_path]) if client_name else dfs_path

        print(f"Iniciando uma nova sessão de upload para: {dfs_path}")

        # CORREÇÃO: Passa o datanode_registry para a sessão
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
        dfs_path = "".join([client_name, dfs_path]) if client_name else dfs_path

        print(f"[CopyService] Iniciando sessão de download para: {dfs_path}")
        entry_info = self._metadata_repo.get_entry(dfs_path)
        if not entry_info or entry_info.get("type") != "file":
            raise FileNotFoundError(f"Arquivo não encontrado: {dfs_path}")
        
        block_list = entry_info.get("blocks", [])
        if not block_list:
            return {"session_uri": None, "message": "Arquivo vazio."}

        # CORREÇÃO: Não precisa mais passar o datanode_client
        session = DownloadSession(self._daemon, block_list, self._datanode_registry)
        session_uri = self._daemon.register(session)
        return {"session_uri": str(session_uri)}