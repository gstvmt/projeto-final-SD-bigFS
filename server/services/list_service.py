import Pyro5.api

@Pyro5.api.expose
class ListService:
    def __init__(self, metadata_repo):
        self._metadata_repo = metadata_repo
        print("ListService inicializado.")

    def ls(self, path, client_name, long_format=False):
        """
        Simula o comando 'ls'.
        Parametros:
            path: O caminho do diretório a ser listado.
            long_format: Se True, retorna detalhes (simula o 'ls -l').
        Retorno:
            Uma lista de nomes de arquivos/diretórios ou uma lista de dicionários com detalhes.
        """
        path = "".join(["/", client_name, path]) if client_name else path
        path = path.rstrip("/")

        print(f"[ListService] Executando ls para o caminho: '{path}' (long_format={long_format})")
        dir_info = self._metadata_repo.get_entry(path)

        if not dir_info or dir_info.get("type") != "dir":
            raise FileNotFoundError(f"Diretório não encontrado: {path}")

        children_names = dir_info.get("children", [])
        
        if not long_format:
            return sorted(children_names)
        
        # Lógica para o 'ls -l'
        detailed_list = []
        for name in children_names:
            child_path = f"{path}/{name}".replace("//", "/")
            child_info = self._metadata_repo.get_entry(child_path)
            if child_info:
                detailed_list.append({
                    "name": name,
                    "type": child_info.get("type"),
                    "size": child_info.get("size", 0)
                })
        return detailed_list