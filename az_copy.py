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
            '--check-length',
            '--put-md5'
        ]
        return command
    
    def build_login_command(self):
        command = [
            self.azcopy_path,
            'login',
            '--service-principal',
            '--certificate-path', './azure_sp_combined.pem',
            '--application-id', '559f7656-bc7f-42e1-b8ad-23409ba5731a',
            '--tenant-id', '65b9116f-bba2-4b02-a238-6e844f727c03'
        ]
        # azcopy login --service-principal --certificate-path ./azure_sp_combined.pem  --application-id 559f7656-bc7f-42e1-b8ad-23409ba5731a --tenant-id 65b9116f-bba2-4b02-a238-6e844f727c03
        return command
    
    def copy_to_azure(self, source_path, destination_url):
        """Copia o arquivo para o Azure usando o AzCopy com limite de rede e suporte a recomeço automático."""
        self.logger.log_info(f"copiando '{source_path}' para '{destination_url}'.")
        command = self.build_command(source_path, destination_url)
        command_login = self.build_login_command()

        attempt = 0
        while attempt < self.retries:
            try:
                subprocess.run(command_login, check=True)
                subprocess.run(command, check=True)
                self.logger.log_info(f"Arquivo '{source_path}' copiado com sucesso para '{destination_url}'.")
                
                break
            except subprocess.CalledProcessError as e:
                attempt += 1
                self.logger.log_error(f"Erro ao executar AzCopy para o arquivo '{source_path}', tentativa {attempt}/{self.retries}: {e}")
                
                if attempt < self.retries:
                    self.logger.log_info(f"Tentando novamente em {self.retry_delay} segundos...")
                    time.sleep(self.retry_delay)
                else:
                    self.logger.log_error(f"Todas as tentativas falharam para o arquivo '{source_path}'.")

                    return False
        return True