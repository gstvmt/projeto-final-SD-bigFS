import Pyro5.api
import os
import base64

'''
Esse arquivo implementa o cliente do sistema de arquivos distribuido.
'''

class Client():
    def __init__(self, ns_ip, ns_port):
        # getting server remote object
        self.ns = Pyro5.api.locate_ns(host=ns_ip, port=ns_port)
        self.uri = self.ns.lookup("fs_server")
        self.server = Pyro5.api.Proxy(self.uri)

        # virtual path
        self.current_path = None

        # comand dictionary
        self.cmd_dict = {
            "ls": self.ls,
            "cp": self.cp,
            "rm": self.rm
        }

        # user and password atributes - autentication
        self.user = ""
        self.password = ""
        # log in, and get the identification hash  
        while True:
            try:
                init_method = input("Bigfs NFS - Choose one method to start:\n 1 - Login (login)\n 2 - Register (register)\n 3 - Exit (exit)\n--->")
                if init_method == "login":
                    print("Login")
                    self.user = input("User: ")
                    self.password = input("Password: ")
                    self.current_path = self.server.login_user(self.user, self.password)
                elif init_method == "register":
                    print("Register")
                    self.user = input("User: ")
                    self.password = input("Password: ")
                    self.current_path = self.server.register_user(self.user, self.password)
                elif init_method == "exit":
                    print("Exit")
                    exit()
                else:
                    print("Invalid option")
                    exit()
                break
            except Exception as e:
                print(e)

    
    def run(self):
        # loop para receber os comandos do usuario
        while True:
            try:
                self.cmd = input(self.current_path + '$ ')
                self.cmd = self.cmd.split()
                if self.cmd[0] == "exit":
                    break
                self.cmd_dict.get(self.cmd[0])(self.cmd[1:])
            except KeyboardInterrupt:
                return
            except Exception as e:
                print(e)

    # implementa o comando ls usando caminhos relativos
    # TODO: adicionar suporte a caminhos absolutos
    def ls(self, path):
        if len(path) > 1:
            print("Invalid number of arguments")
            return
        else:
            try:
                target_path = path[0] if len(path) == 1 else "."
                l = self.server.ls(target_path, self.user)
                for i in l:
                    print(i)
            except Exception as e:
                print(e)
                return
    
    # implementa o comando cp
    # os caminhos locais devem ser absolutos, ex: /home/user/..., isso diferencia do caminho remoto
    # que deve sera algo como /NFS/... podendo ser relativo ou absoluto
    def cp(self, paths):
        local_path = None
        remote_path = None
        for p in paths:
            if os.path.exists(os.path.dirname(p)):
                local_path = p
            else:
                remote_path = p
        
        if local_path is None or remote_path is None:
            print("Argumentos invalidos | Se espera um caminho local absoluto")
            return
        try:
            if local_path == paths[0]: # local_path = source
                endpoint = self.server.cp_from(remote_path, self.user)
                # comeca a enviar os chuncks
                with open(local_path, "rb") as f:
                    while True:
                        chunk = f.read(1024*64) # 64kB
                        eof = self.server.receive_chunk(chunk, endpoint)
                        if eof:
                            break
            else: # local_path = dst
                endpoint = self.server.cp_to(remote_path, self.user)
                # comeca a receber os chuncks
                with open(local_path, "wb") as f:
                    for chunk in self.server.send_chunk(endpoint):
                        chunk = base64.b64decode(chunk.get("data"))
                        f.write(chunk)
                
        except Exception as e:
            print(e)
            return
        print("Transferencia concluida")
        return
    

    # implementa o comando rm
    # remove um arquivo ou mais arquivos remotos
    def rm(self, path):
        try:
            self.server.rm(path, self.user)
        except Exception as e:
            print(e)
            return
        print("Arquivo removido")
        return

def main():
    cl = Client('0.0.0.0', 9090)
    cl.run()

if __name__ == "__main__":
    main()
