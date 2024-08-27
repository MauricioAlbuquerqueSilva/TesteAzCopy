import hashlib
import os
import time
import tempfile
import subprocess
from logger_manager import LoggerManager

class AzCopy:
    def __init__(self, azcopy_path, file_system_manager, logger=None, retries=5, retry_delay=30):
        """
        :param azcopy_path: Caminho para o executável do AzCopy.
        :param logger: Instância do LoggerManager.
        :param retries: Número máximo de tentativas em caso de falha.
        :param retry_delay: Intervalo entre tentativas em segundos.
        """
        self.azcopy_path = azcopy_path
        self.logger = logger or LoggerManager('azcopy.log')
        self.retries = retries
        self.retry_delay = retry_delay
        self.file_system_manager = file_system_manager

    def build_command(self, source_path, destination_url):
        """Constrói o comando do AzCopy com as opções fornecidas."""
        command = [
            self.azcopy_path,
            'copy',
            source_path,
            destination_url,
            '--recursive',
            '--check-length'
        ]
        return command

    def get_remote_file_hash(self, destination_url, file_name, hash_algo='sha256'):
        """
        Obtém o hash do arquivo remoto no Azure Blob Storage usando o AzCopy.
        :param destination_url: URL do Blob no Azure.
        :param hash_algo: Algoritmo de hash a ser utilizado.
        :return: Hash do arquivo remoto.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_path = os.path.join(temp_dir, "temp_file")
        
            command = [
                self.azcopy_path,
                'copy',
                destination_url,
                temp_file_path,
                '--recursive'
            ]
            
            try:
                subprocess.run(command, check=True)
                self.logger.log_info(f"Arquivo '{destination_url}' baixado para '{temp_file_path}'.")
                hash_func = hashlib.new(hash_algo)
                with open(f'{temp_file_path}/inbound/{file_name}', 'rb') as f:
                    while chunk := f.read(8192):
                        hash_func.update(chunk)

                return hash_func.hexdigest()
            except subprocess.CalledProcessError as e:
                self.logger.log_error(f"Erro ao baixar o arquivo '{destination_url}': {e}")
                raise e

    def copy_to_azure(self, source_path, destination_url):
        """Copia o arquivo para o Azure usando o AzCopy com limite de rede e suporte a recomeço automático."""
        self.logger.log_info(f"copiando '{source_path}' para '{destination_url}'.")
        command = self.build_command(source_path, destination_url)
        local_hash = self.file_system_manager.generate_file_hash(source_path)
        attempt = 0
        while attempt < self.retries:
            try:
                subprocess.run(command, check=True)
                self.logger.log_info(f"Arquivo '{source_path}' copiado com sucesso para '{destination_url}'.")
                
                remote_hash = self.get_remote_file_hash(destination_url, source_path.split('/')[-1])
                if local_hash == remote_hash:
                    self.logger.log_info(f"Integridade do arquivo '{source_path}' verificada com sucesso.")
                else:
                    self.logger.log_error(f"Falha na verificação de integridade para o arquivo '{source_path}'.")
                    raise ValueError("Hashes não coincidem.")
                break
            except subprocess.CalledProcessError as e:
                attempt += 1
                self.logger.log_error(f"Erro ao executar AzCopy para o arquivo '{source_path}', tentativa {attempt}/{self.retries}: {e}")
                
                if attempt < self.retries:
                    self.logger.log_info(f"Tentando novamente em {self.retry_delay} segundos...")
                    time.sleep(self.retry_delay)
                else:
                    self.logger.log_error(f"Todas as tentativas falharam para o arquivo '{source_path}'.")
                    raise e
