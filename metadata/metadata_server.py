# metadata_server.py

import threading
import json
import os
import time
import Pyro5.api
import Pyro5.server
from kafka import KafkaProducer

# --- Configuração ---
STATE_FILE = "metadata_storage.json"
KAFKA_SERVERS = ['localhost:9092']
INVALIDATION_TOPIC = "metadata_invalidation_events"

@Pyro5.api.expose
class MetadataServerService:
    """
    O cérebro do sistema de arquivos distribuído.
    Mantém a estrutura de diretórios e a localização dos blocos de dados.
    """
    def __init__(self):
        self._filesystem = {}
        self._lock = threading.Lock()
        
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            print(f"AVISO: Não foi possível conectar ao Kafka Producer: {e}. A invalidação de cache estará desativada.")
            self._producer = None
            
        self._load_state_from_disk()
        print("Metadata Server inicializado.")

    def _load_state_from_disk(self):
        """Carrega o estado do sistema de arquivos do disco na inicialização."""
        with self._lock:
            if os.path.exists(STATE_FILE):
                print(f"Carregando estado do arquivo: {STATE_FILE}")
                try:
                    with open(STATE_FILE, 'r') as f:
                        self._filesystem = json.load(f)
                except (IOError, json.JSONDecodeError) as e:
                    print(f"Erro ao carregar estado: {e}. Iniciando com estado padrão.")
                    self._initialize_default_state()
            else:
                print("Nenhum arquivo de estado encontrado. Iniciando com estado padrão.")
                self._initialize_default_state()
            
            self._save_state_to_disk() # Garante que o arquivo seja criado se não existir

    def _initialize_default_state(self):
        """Cria a estrutura de diretório raiz se o sistema estiver vazio."""
        self._filesystem = {
            "/": {"type": "dir", "children": []}
        }

    def _save_state_to_disk(self):
        """Salva o estado atual do sistema de arquivos em disco. Deve ser chamado dentro de um lock."""
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump(self._filesystem, f, indent=4)
        except IOError as e:
            print(f"ERRO CRÍTICO ao salvar estado no disco: {e}")

    def _publish_invalidation(self, path: str):
        """Publica um evento de invalidação de cache no Kafka."""
        if not self._producer:
            return
            
        message = {"path": path, "timestamp": time.time()}
        print(f"[Cache Invalidation] Publicando para o caminho: {path}")
        self._producer.send(INVALIDATION_TOPIC, message)

    @Pyro5.api.expose
    def get_fs_entry(self, path: str):
        """Retorna os metadados para um dado caminho."""
        with self._lock:
            print(f"[RMI Call] get_fs_entry para: {path}")
            # Retorna uma cópia para evitar que clientes modifiquem o estado interno
            return self._filesystem.get(path, None)

    @Pyro5.api.expose
    def add_fs_entry(self, path: str, entry_data: dict) -> bool:
        """Adiciona uma nova entrada (arquivo/diretório) no sistema."""
        with self._lock:
            print(f"[RMI Call] add_fs_entry para: {path}")
            
            # Validação
            if path in self._filesystem:
                print(f"--> O caminho '{path}' já existe.")
                return True
            
            parent_path = os.path.dirname(path)
            if parent_path not in self._filesystem or self._filesystem[parent_path]['type'] != 'dir':
                print(f"--> Falha: O diretório pai '{parent_path}' não existe.")
                return False
            
            # Modificação
            self._filesystem[path] = entry_data
            filename = os.path.basename(path)
            self._filesystem[parent_path]['children'].append(filename)
            
            # Persistência e Notificação
            self._save_state_to_disk()
            self._publish_invalidation(parent_path) # Invalida o cache do diretório pai
            self._publish_invalidation(path)      # Invalida o cache do próprio caminho
            
            print(f"--> Sucesso: Entrada '{path}' adicionada.")
            return True

    @Pyro5.api.expose
    def remove_fs_entry(self, path: str) -> bool:
        """Remove uma entrada do sistema."""
        with self._lock:
            print(f"[RMI Call] remove_fs_entry para: {path}")
            
            # Validação
            if path not in self._filesystem:
                print(f"--> Falha: O caminho '{path}' não existe.")
                return False
            if path == "/":
                print("--> Falha: Não é permitido remover o diretório raiz.")
                return False
            
            entry_data = self._filesystem[path]
            if entry_data['type'] == 'dir' and entry_data['children']:
                # Simplificação: não permite remover diretórios não vazios.
                print(f"--> Falha: O diretório '{path}' não está vazio.")
                return False
                
            # Modificação
            del self._filesystem[path]
            parent_path = os.path.dirname(path)
            filename = os.path.basename(path)
            if parent_path in self._filesystem and filename in self._filesystem[parent_path]['children']:
                self._filesystem[parent_path]['children'].remove(filename)

            # Persistência e Notificação
            self._save_state_to_disk()
            self._publish_invalidation(parent_path) # Invalida o cache do diretório pai
            self._publish_invalidation(path)      # Invalida o cache do caminho removido
            
            print(f"--> Sucesso: Entrada '{path}' removida.")
            return True

if __name__ == "__main__":
    # Inicia o serviço
    daemon = Pyro5.server.Daemon()
    metadata_service = MetadataServerService()
    uri = daemon.register(metadata_service, objectId="MetadataService")
    ns = Pyro5.api.locate_ns()  # Localiza o Name Server Pyro
    ns.register("MetadataService", uri)  # Registra o serviço no Name Server
    
    print("="*50)
    print(" Metadata Server pronto e rodando.")
    print(f"   Serviço disponível em: {uri}")
    print("="*50)
    
    daemon.requestLoop()