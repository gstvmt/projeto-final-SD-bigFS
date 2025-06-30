import Pyro5.api
import threading

# File: server/shared/metadata_repo.py
class MetadataRepository:
    def __init__(self, remote_metadata_service_uri):
        """
        Inicializa o repositório com um cache local e um proxy para o serviço remoto.
        
        :param local_cache: Um dicionário simples para servir de cache em memória.
        :param remote_metadata_service_proxy: Um proxy Pyro para o Servidor de Metadados.
        """
        self._cache = {}
        self._remote_service = Pyro5.api.Proxy(remote_metadata_service_uri) 
        self._lock = threading.Lock()  # Protege o acesso ao cache
        print("MetadataRepository inicializado.")

    def get_entry(self, path):
        """
        Obtém informações de uma entrada (arquivo/diretório).
        Implementa a lógica: "tenta no cache, senão busca remotamente e atualiza o cache".
        """
        # 1. Tenta buscar no cache local
        self._remote_service._pyroClaimOwnership()
        if path in self._cache:
            print(f"[MetadataRepository] CACHE HIT para: {path}")
            return self._cache[path]
        
        # 2. Se não achou, busca no serviço remoto
        print(f"[MetadataRepository] CACHE MISS para: {path}. Buscando remotamente...")
        remote_data = self._remote_service.get_fs_entry(path)
        
        # 3. Adiciona no cache local para a próxima vez
        if remote_data:
            self._cache[path] = remote_data
        
        return remote_data

    def remove_entry(self, path):
        """Remove uma entrada do sistema e invalida o cache."""
        # Invalida o cache primeiro
        self._remote_service._pyroClaimOwnership()
        if path in self._cache:
            del self._cache[path]
            print(f"[MetadataRepository] Cache invalidado para: {path}")
            
        # Chama o serviço remoto para fazer a remoção real
        return self._remote_service.remove_fs_entry(path)

    def add_entry(self, path, entry_data):
        """Adiciona uma nova entrada no sistema e no cache."""
        # Chama o serviço remoto para adicionar
        self._remote_service._pyroClaimOwnership()
        success = self._remote_service.add_fs_entry(path, entry_data)
        
        # Se foi bem-sucedido, atualiza o cache local
        if success:
            self._cache[path] = entry_data
            print(f"[MetadataRepository] Cache populado para nova entrada: {path}")
        
        return success
    
    def invalidate_cache(self, path: str):
        """
        Remove de forma segura uma entrada específica do cache local.
        """
        with self._lock:
            if path in self._cache:
                del self._cache[path]
                print(f"[MetadataRepository] CACHE INVALIDADO para o caminho: {path}")
            else:
                # Isso é normal, o cache pode não ter a entrada
                print(f"[MetadataRepository] Invalidação para '{path}' recebida, mas não estava no cache.")