# shared/datanode_registry.py

import threading

class DataNodeRegistry:
    """
    Componente thread-safe para manter uma lista atualizada de DataNodes ativos.
    Esta classe será instanciada no server para manter sua visão do cluster de datanodes.
    """
    def __init__(self):
        self._active_nodes = set()
        self._lock = threading.Lock()
        print("[DataNodeRegistry] Instância criada.")

    def bootstrap(self, initial_nodes: list):
        """Popula o registro com uma lista inicial de nós."""
        with self._lock:
            self._active_nodes.update(initial_nodes)
        print(f"[DataNodeRegistry] Bootstrap concluído. {len(initial_nodes)} nós carregados.")

    def add_node(self, node_uri: str):
        """Adiciona um novo nó à lista de ativos."""
        with self._lock:
            self._active_nodes.add(node_uri)
        print(f"[DataNodeRegistry] Nó adicionado: {node_uri}. Nós ativos: {len(self._active_nodes)}")

    def remove_node(self, node_uri: str):
        """Remove um nó da lista de ativos."""
        with self._lock:
            self._active_nodes.discard(node_uri) 
        print(f"[DataNodeRegistry] Nó removido: {node_uri}. Nós ativos: {len(self._active_nodes)}")
        
    def get_available_nodes(self) -> list:
        """Retorna uma cópia da lista de nós ativos."""
        with self._lock:
            return list(self._active_nodes)

    def get_all(self) -> set:
        """Retorna uma cópia do conjunto de nós ativos."""
        with self._lock:
            return self._active_nodes.copy()