# Entrega 2 BigFS

Passos para executar o sistema:

## Subindo o name server

O seguinte comando expoe o name server em todas as interfaces
```
python3 -m Pyro5.nameserver -n 0.0.0.0
```
## Executando o server

Para executar o server basta executar o seguinte comando no diretorio com os arquivos implementados
```
python3 server_master.py
```

## Executando os workers

Os workers podem ser executados em maquinas diferentes, para subilos basta usar o comando:
```
python3 worker.py
```

## Executando o cliente

Em uma maquina a consumir os serviços do sistema distribuido, execute o seguinte comando:
```
python3 client.py
```