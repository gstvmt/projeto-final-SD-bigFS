import Pyro5.api
import shlex
import os
import time
import base64

class FileSystemClient:
    """
    Uma classe cliente que abstrai toda a comunicação com o sistema de arquivos
    distribuído por meio do API Gateway.
    """
    def __init__(self, nameserver):
        self.client_id = f"Client-{int(time.time())}"
        print("Conectando ao API Gateway via Name Server...")
        # Localiza o API Gateway usando seu nome lógico no Name Server do Pyro
        gateway_uri = nameserver.lookup("APIGateway")
        if not gateway_uri:
            raise ConnectionError("Não foi possível encontrar o 'APIGateway' no Name Server.")
        
        self.gateway_proxy = Pyro5.api.Proxy(gateway_uri)
        print(f"Conectado com sucesso ao API Gateway em: {gateway_uri}")

    def list_directory(self, dfs_path, long_format=False):
        """Abstrai a chamada para o serviço 'ls'."""
        return self.gateway_proxy.forward_request("ListService", "ls", dfs_path, long_format=long_format)

    def remove_file(self, dfs_path):
        """Abstrai a chamada para o serviço 'rm'."""
        return self.gateway_proxy.forward_request("RemoveService", "rm", dfs_path)

    def upload_file(self, local_path, dfs_path):
        """
        Abstrai o fluxo completo de upload assíncrono.
        """
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Arquivo local não encontrado: {local_path}")

        file_size = os.path.getsize(local_path)
        file_info = {"size": file_size}
        
        # 1. Inicia a sessão de upload (Plano de Controle)
        response = self.gateway_proxy.forward_request("CopyService", "initiate_upload", dfs_path, self.client_id)
        session_uri = response["session_uri"]
        endpoint = response["endpoint"]
        
        print(f"Sessão de upload iniciada. Conectando a {session_uri}")
        
        # 2. Conecta à sessão e envia os dados em blocos (Plano de Dados)
        with Pyro5.api.Proxy(session_uri) as upload_proxy:
            try:
                with open(local_path, "rb") as f:
                    while True:
                        chunk = f.read(64 * 1024)
                        if not chunk:
                            upload_proxy.close(endpoint)
                            break 
                        endpoint = upload_proxy.write_chunk(chunk, endpoint)
                
                # 3. Finaliza (commita) a transação
                print("Todos os blocos enviados. Finalizando o upload...")
                return upload_proxy.commit(total_size=file_size)
            except Exception as e:
                print(f"Erro durante a transferência, abortando sessão: {e}")
                upload_proxy.abort()
                raise # Propaga o erro

    def download_file(self, dfs_path, local_path):
        """
        Abstrai o fluxo completo de download assíncrono.
        """
        print(f"Iniciando download de '{dfs_path}' para '{local_path}'...")
        
        # 1. Inicia a sessão de download
        response = self.gateway_proxy.forward_request("CopyService", "initiate_download", dfs_path, self.client_id)
        session_uri = response.get("session_uri")
        
        # Lida com o caso de arquivo vazio
        if not session_uri:
            with open(local_path, "wb") as f: pass
            return {"status": "success", "message": "Arquivo vazio baixado com sucesso."}

        print(f"Sessão de download iniciada. Conectando a {session_uri}")
        
        # 2. Conecta à sessão e baixa os dados em streaming
        try:
            with Pyro5.api.Proxy(session_uri) as download_proxy, open(local_path, "wb") as f:
                while True:
                    chunk = download_proxy.read_chunk()
                    chunk = base64.b64decode(chunk.get('data')) if isinstance(chunk, dict) else chunk
                    if chunk is None:
                        break # Fim da transferência
                    f.write(chunk)
            
            return {"status": "success", "message": f"Download concluído. Arquivo salvo em '{local_path}'."}
        except Exception as e:
            print(f"Erro durante o download: {e}")
            # Em uma implementação real, o download_proxy também poderia ter um método 'abort'
            raise

def print_ls_result(result):
    """Formata a saída do comando 'ls'."""
    if not result: return
    if isinstance(result[0], dict):
        print(f"{'TYPE':<6} {'SIZE':>10} {'NAME'}")
        print("-"*28)
        for item in result:
            print(f"{item.get('type', 'N/A'):<6} {item.get('size', 0):>10} {item.get('name', 'N/A')}")
    else:
        print("  ".join(result))

def main_shell(client: FileSystemClient):
    """O laço principal que simula o shell."""
    print("\nShell do Sistema de Arquivos Distribuído. Digite 'exit' para sair.")
    print("Use o prefixo 'dfs:' para caminhos remotos. Ex: 'ls dfs:/'")

    while True:
        try:
            cmd_line = input("fs-shell> ")
            if cmd_line.strip().lower() == 'exit':
                break
            
            parts = shlex.split(cmd_line)
            if not parts:
                continue

            command = parts[0].lower()
            
            if command == 'ls':
                path = parts[1] if len(parts) > 1 else "dfs:/"
                if not path.startswith("dfs:"): print("Erro: 'ls' requer um caminho do DFS (ex: dfs:/)."); continue
                dfs_path = path.split("dfs:", 1)[1] or "/"
                result = client.list_directory(dfs_path, long_format=("-l" in parts))
                print_ls_result(result)

            elif command == 'rm':
                if len(parts) != 2 or not parts[1].startswith("dfs:"): print("Uso: rm dfs:<caminho>"); continue
                dfs_path = parts[1].split("dfs:", 1)[1]
                result = client.remove_file(dfs_path)
                print(result['message'])

            elif command == 'cp':
                if len(parts) != 3: print("Uso: cp <origem> <destino>"); continue
                source, dest = parts[1], parts[2]
                
                if not source.startswith("dfs:") and dest.startswith("dfs:"):
                    # Upload
                    dfs_path = dest.split("dfs:", 1)[1]
                    result = client.upload_file(source, dfs_path)
                    print(result['message'])
                elif source.startswith("dfs:") and not dest.startswith("dfs:"):
                    # Download
                    dfs_path = source.split("dfs:", 1)[1]
                    result = client.download_file(dfs_path, dest)
                    print(result['message'])
                else:
                    print("Erro: Cópia deve ser entre um caminho local e um do DFS (com prefixo 'dfs:').")
            else:
                print(f"Comando desconhecido: {command}. Comandos disponíveis: ls, cp, rm, exit")

        except Exception as e:
            print(f"ERRO DE EXECUÇÃO: {e}")

if __name__ == "__main__":
    try:
        # Encontra o Name Server automaticamente na rede
        nameserver = Pyro5.api.locate_ns()
        # Cria a instância do cliente que se conecta ao Gateway
        fs_client = FileSystemClient(nameserver)
        # Inicia o shell
        main_shell(fs_client)
    except Pyro5.errors.NamingError:
        print("Erro: Não foi possível localizar o Pyro Name Server.")
        print("Certifique-se de que ele está rodando com o comando: python -m Pyro5.nameserver")
    except Exception as e:
        print(f"Ocorreu um erro inesperado na inicialização: {e}")