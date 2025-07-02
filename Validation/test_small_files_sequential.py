# Cenário 1
import time
from utils import create_test_files, calculate_checksum, drop_caches
import sys
import os
from monitor_resources import ResourceMonitor

# Adiciona o diretório pai ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from client import Client

def test_sequential_transfer():
    # Configuração
    local_dir = "test_files_small"
    num_files = 100
    file_size_kb = 512
    monitor = ResourceMonitor()
    
    # Criar arquivos de teste
    print("Criando arquivos de teste...")
    test_files = create_test_files(local_dir, num_files, file_size_kb)
    drop_caches()
    
    # Inicializar cliente
    client = Client()
    
    # Testar transferência
    results = []
    total_time = 0
    
    monitor.start()
    for filepath in test_files:
        filename = os.path.basename(filepath)
        remote_path = "remoto:."
        
        # Calcular checksum local
        local_checksum = calculate_checksum(filepath)
        
        # Medir tempo de transferência
        start_time = time.time()
        result = client.copy([filepath, remote_path])
        transfer_time = time.time() - start_time
        
        if "sucesso" not in result:
            print(f"Falha ao transferir {filename}: {result}")
            results.append((filename, False, transfer_time, "Falha na transferência"))
            continue
        
        # Transferir de volta para verificar checksum
        remote_path = "remoto:" + filename
        local_copy_path = os.path.join(local_dir, f"copy_testfile")
        result = client.copy([remote_path, local_copy_path])

        
        if "sucesso" not in result:
            print(f"Falha ao verificar {filename}: {result}")
            results.append((filename, False, transfer_time, "Falha na verificação"))
            continue
        
        # Calcular checksum do arquivo transferido
        local_copy_path = local_copy_path + "/" + filename
        remote_checksum = calculate_checksum(local_copy_path)
        
        # Verificar integridade
        success = local_checksum == remote_checksum
        results.append((filename, success, transfer_time, 
                       "Checksum OK" if success else "Checksum falhou"))
        
        total_time += transfer_time
        os.remove(local_copy_path)

    monitor.stop()
    monitor.show_results()
    
    # Relatório
    print("\n=== Resultados ===")
    success_count = sum(1 for r in results if r[1])
    print(f"Arquivos transferidos com sucesso: {success_count}/{num_files}")
    print(f"Tempo total: {total_time:.2f} segundos")
    print(f"Latência média: {total_time/num_files:.4f} segundos por arquivo")
    
    # Exibir falhas
    failures = [r for r in results if not r[1]]
    if failures:
        print("\nFalhas:")
        for f in failures:
            print(f"- {f[0]}: {f[3]}")

if __name__ == "__main__":
    test_sequential_transfer()