import os
import hashlib
from logger_manager import LoggerManager

class FileSystemManager:
    def __init__(self, directory_to_watch, logger=None):
        self.directory_to_watch = directory_to_watch
        self.logger = logger or LoggerManager('file_system_manager.log')

    def generate_file_hash(self, file_path, hash_algo='sha256'):
        """Gera o hash do arquivo local usando o algoritmo especificado."""
        try:
            hash_func = hashlib.new(hash_algo)
            with open(file_path, 'rb') as f:
                while chunk := f.read(8192):
                    hash_func.update(chunk)
            file_hash = hash_func.hexdigest()
            self.logger.log_info(f"Hash do arquivo '{file_path}' gerado com sucesso: {file_hash}")
            return file_hash
        except Exception as e:
            self.logger.log_error(f"Erro ao gerar hash do arquivo '{file_path}': {e}")
            raise e

    def file_exists(self, file_path):
        """Verifica se o arquivo existe."""
        exists = os.path.exists(file_path)
        self.logger.log_info(f"Verificação de existência do arquivo '{file_path}': {'Existe' if exists else 'Não existe'}")
        return exists

    def check_read_permissions(self, file_path):
        """Verifica se o arquivo tem permissões de leitura."""
        readable = os.access(file_path, os.R_OK)
        self.logger.log_info(f"Verificação de permissão de leitura do arquivo '{file_path}': {'Permitido' if readable else 'Negado'}")
        return readable

    def check_write_permissions(self, file_path):
        """Verifica se o arquivo tem permissões de escrita."""
        writable = os.access(file_path, os.W_OK)
        self.logger.log_info(f"Verificação de permissão de escrita do arquivo '{file_path}': {'Permitido' if writable else 'Negado'}")
        return writable

    def validate_file(self, file_path):
        """Realiza todas as validações necessárias para o arquivo."""
        try:
            if not self.file_exists(file_path):
                raise FileNotFoundError(f"O arquivo '{file_path}' não foi encontrado.")
            
            if not self.check_read_permissions(file_path):
                raise PermissionError(f"Permissão de leitura negada para o arquivo '{file_path}'.")
    
            if not self.check_write_permissions(file_path):
                raise PermissionError(f"Permissão de escrita negada para o arquivo '{file_path}'.")
            
            self.logger.log_info(f"Arquivo '{file_path}' validado com sucesso.")
        
        except (FileNotFoundError, PermissionError) as e:
            self.logger.log_error(f"Erro na validação do arquivo '{file_path}': {e}")
            raise e

    def process_file(self, file_path, az_copy, destination_url):
        """Valida e processa o arquivo, copiando-o para o Azure."""
        try:
            self.validate_file(file_path)
        
            az_copy.copy_to_azure(file_path, destination_url)
        except Exception as e:
            self.logger.log_error(f"Erro ao processar o arquivo '{file_path}': {e}")
            raise e
