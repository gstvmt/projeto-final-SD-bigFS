import Pyro5.api
from concurrent.futures import ThreadPoolExecutor

@Pyro5.api.expose
class RemoveService:
    def __init__(self, metadata_repo):
        """
        Inicializa o serviço de remoção apenas com a dependência do
        repositório de metadados.
        """
        self._metadata_repo = metadata_repo
        print("RemoveService (versão simplificada) inicializado.")

    def _delete_block_from_node(self, datanode_uri, block_id):
        """
        Função auxiliar para se conectar a um DataNode e deletar um bloco.
        Isso encapsula a chamada Pyro individual.
        """
        try:
            # Cria o proxy para o DataNode sob demanda
            with Pyro5.api.Proxy(datanode_uri) as datanode_proxy:
                print(f"--> Conectando a {datanode_uri} para deletar bloco {block_id}")
                return datanode_proxy.delete_block(block_id)
        except Exception as e:
            print(f"ERRO: Falha na comunicação com {datanode_uri} ao deletar bloco {block_id}: {e}")
            return False

    def rm(self, path):
        """
        Remove um arquivo do sistema. A lógica de comunicação com os DataNodes
        está agora diretamente dentro deste método.
        """
        print(f"[RemoveService] Iniciando remoção para: '{path}'")

        # 1. Pegar a entrada do arquivo no servidor de metadados
        entry_info = self._metadata_repo.get_entry(path)

        # 2. Validação
        if not entry_info:
            raise FileNotFoundError(f"Arquivo ou diretório não encontrado: {path}")

        if entry_info["type"] == "dir":
            if entry_info.get("children"):
                raise IsADirectoryError(f"Não é possível remover '{path}': é um diretório não vazio.")
            print(f"Removendo diretório '{path}'...")
            
        # 3. Remoção paralela dos blocos
        if entry_info.get("type") == "file" and "blocks" in entry_info:
            with ThreadPoolExecutor(max_workers=10) as executor:
                # Cria uma lista de tarefas. Cada tarefa é uma chamada para deletar uma réplica de um bloco.
                tasks = []
                for block in entry_info["blocks"]:
                    for replica_uri in block.get("replicas", []):
                        tasks.append(executor.submit(self._delete_block_from_node, replica_uri, block["block_id"]))
                
                print(f"Enviando {len(tasks)} comandos de exclusão de blocos em paralelo...")
                
                # Verifica se todas as tarefas foram bem-sucedidas
                results = [future.result() for future in tasks]
                if not all(results):
                    # Em um sistema real, aqui teríamos uma lógica mais robusta
                    raise IOError("Falha ao deletar um ou mais blocos de dados. Operação de remoção abortada.")

        # 4. Atualizar a tabela de metadados
        print(f"Blocos de dados para '{path}' removidos. Atualizando metadados...")
        success = self._metadata_repo.remove_entry(path)

        if success:
            return {"status": "success", "message": f"'{path}' foi removido com sucesso."}
        else:
            raise SystemError(f"ALERTA DE INCONSISTÊNCIA: Dados de '{path}' removidos, mas falha ao limpar metadados.")