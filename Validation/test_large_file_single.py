# Cenário 4

from utils import generate_random_file, calculate_checksum, drop_caches
from monitor_resources import KafkaResourceMonitor
import Pyro5.api
import time
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../client')))
from client import FileSystemClient

def test_large_file_transfer():
    """
        Executa um teste de transferência de arquivo grande entre cliente e servidor DFS (Distributed File System),
        monitorando os recursos durante a operação e verificando a integridade do arquivo.

        Cenário:
            1. Gera um arquivo com dados aleatórios localmente
            2. Inicia o monitoramento de recursos via Kafka
            3. Realiza o upload do arquivo para o DFS
            4. Para o monitoramento
            5. Opcionalmente verifica a integridade do arquivo transferido
            6. Limpa os arquivos temporários
            7. Exibe os resultados do monitoramento

        Parâmetros:
        Nenhum parâmetro explícito, mas utiliza constantes internas:
            - file_size_mb: Tamanho do arquivo de teste em MB (padrão: 5120 MB = 5GB)
            - local_file: Nome do arquivo local temporário (padrão: "large_file.txt")
            - remote_path: Caminho remoto no DFS (padrão: "dfs:/large_file.txt")

        Retorno:
            None (exibe resultados diretamente na saída padrão e via gráficos)

        Fluxo detalhado:
            1. Preparação:
                - Gera arquivo de teste com dados aleatórios
                - Limpa caches do sistema para medição mais precisa
                - Conecta ao nameserver Pyro e inicializa cliente DFS

            2. Execução com monitoramento:
                - Inicia monitoramento de recursos via Kafka
                - Executa upload do arquivo
                - Encerra monitoramento

            3. Verificação (opcional):
                - Calcula checksum MD5 do arquivo original
                - Baixa o arquivo do DFS
                - Calcula checksum do arquivo baixado
                - Compara os checksums para verificar integridade

            4. Finalização:
                - Remove arquivos temporários
                - Exibe gráficos de desempenho e estatísticas

        Observações:
            - Requer serviço Kafka rodando para monitoramento
            - Requer nameserver Pyro rodando para conexão com DFS
            - Arquivos temporários são automaticamente removidos
            - Verificação de integridade pode ser desligada com verify=False
    """
    # Configuração
    file_size_mb = 518  # 5GB
    local_file = "large_file.txt"
    remote_path = "dfs:/large_file.txt"
    
    # Gerar arquivo grande (pode demorar)
    print(f"Criando arquivo grande de {file_size_mb}MB...")
    generate_random_file(local_file, file_size_mb * 1024)
    drop_caches()
    
    # Inicializar cliente
    nameserver = Pyro5.api.locate_ns()
    client = FileSystemClient(nameserver)

    
    monitor = KafkaResourceMonitor()
    print("Iniciando transferência...")
    monitor.start()
    client.upload_file(local_file, remote_path)
    monitor.stop()

    
    # Verificar integridade
    verify = True
    if verify:
        print("Calculando checksum local...")
        local_checksum = calculate_checksum(local_file)
        
        print("Transferindo de volta para verificação...")
        local_copy = "large_copy"
        remote_path = "dfs:/" + os.path.basename(local_file)
        client.download_file(remote_path, local_copy)
        
        print("Calculando checksum remoto...")
        local_copy = local_copy #+ "/" + os.path.basename(local_file)
        remote_checksum = calculate_checksum(local_copy)
        
        if local_checksum == remote_checksum:
            print("Checksum OK - Arquivo íntegro")
        else:
            print("Checksum falhou - Arquivo corrompido")
        
        os.remove(local_copy)
    
    # Limpar
    os.remove(local_file)
    monitor.show_results()

if __name__ == "__main__":
    test_large_file_transfer()