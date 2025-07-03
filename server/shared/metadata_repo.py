import Pyro5.api
import threading

class MetadataRepository:
    def __init__(self, remote_metadata_service_uri):
        """
        Inicializa o repositório com um cache local e um proxy para o serviço remoto.
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
        # Tenta buscar no cache local
        self._remote_service._pyroClaimOwnership()
        if path in self._cache:
            print(f"[MetadataRepository] CACHE HIT para: {path}")
            return self._cache[path]
        
        # Se não achou, busca no serviço remoto
        print(f"[MetadataRepository] CACHE MISS para: {path}. Buscando remotamente...")
        remote_data = self._remote_service.get_fs_entry(path)
        
        # Adiciona no cache local para a próxima vez
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
        """
        Garante que todos os diretórios pais existam antes de adicionar a entrada final.
        Exemplo: Para '1/teste/teste2/arquivo.txt', cria os diretórios '1', '1/teste', '1/teste/teste2'
        antes de adicionar o 'arquivo.txt'.
        """
        # Divide o caminho por partes
        parts = path.strip('/').split('/')
        current_path = ""
        self._remote_service._pyroClaimOwnership()
        
        for i, part in enumerate(parts):
            current_path += "/" + part

            # Se for a última parte, é o arquivo final
            is_last = (i == len(parts) - 1)

            if not is_last:
                # Criar diretório se ainda não existe
                if current_path not in self._cache:
                    print(f"[MetadataRepository] Criando diretório: {current_path}")
                    dir_entry = {"type": "dir", "children": []}
                    success = self._remote_service.add_fs_entry(current_path, dir_entry)
                    if not success:
                        print(f"[MetadataRepository] Falha ao criar diretório: {current_path}")
                        return False
            else:
                # Adiciona o arquivo ou entrada final
                success = self._remote_service.add_fs_entry(current_path, entry_data)
                if not success:
                    print(f"[MetadataRepository] Falha ao adicionar entrada final: {current_path}")
                    return False

        print(f"[MetadataRepository] Entrada adicionada com sucesso: {path}")
        self._cache[path] = entry_data
        return True
    
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