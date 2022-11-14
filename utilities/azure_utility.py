import os
import logging
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.storage.filedatalake import FileSystemClient, DataLakeFileClient
from urllib.parse import urlparse, unquote


class VirtualFileSystemClient:

    def __init__(self, sas: str = None):
        parsed_sas = urlparse(sas.rstrip('/'))
        # if not '.dfs.' in parsed_sas.hostname:
        #     raise ValueError("Not a valid data lake sas url")

        self.account_url = f'{parsed_sas.scheme}://{parsed_sas.hostname.replace(".blob.",".dfs.")}/'
        self.credential = f'?{parsed_sas.query}'
        directory_structure = parsed_sas.path.lstrip('/').split('/')
        self.file_system_name = directory_structure[0]
        self.root_directory = unquote('/'.join(directory_structure[1:]))
        self.root_directory = f'{self.root_directory}'
        self.file_system_client = FileSystemClient(
            account_url=self.account_url,
            file_system_name=self.file_system_name,
            credential=self.credential)

    def download_dir(self, source: str, destination: str):
        os.makedirs(destination, exist_ok=True)
        source = source if source else self.root_directory
        paths = self.file_system_client.get_paths(source)
        for path in paths:
            if path.is_directory == False:
                file_name = os.path.join(destination,
                                         os.path.relpath(path.name, source))
                if (os.path.exists(file_name)):
                    continue
                file_directory = os.path.dirname(file_name)
                os.makedirs(file_directory, exist_ok=True)

                self.download_file(path.name, file_name)

    def download(self, tempdir: str = "temp") -> str:
        destination = os.path.join(tempdir, self.root_directory)
        file_directory = os.path.dirname(destination)
        os.makedirs(file_directory, exist_ok=True)
        file_client = DataLakeFileClient(
            self.account_url, self.file_system_name, self.root_directory, self.credential)
        with open(destination, "wb") as destination_file:
            download = file_client.download_file()
            download.readinto(destination_file)
        return destination

    def download_file(self, source: str, destination: str):
        file_client = self.file_system_client.get_file_client(source)
        with open(destination, "wb") as destination_file:
            download = file_client.download_file()
            download.readinto(destination_file)

    def upload(self, source: str, destination: str = None, overwrite: bool = True):
        if(destination is None):
            destination = self.root_directory
            
        if (os.path.isdir(source)):
            for root, dirs, files in os.walk(source):
                for name in files:
                    dir_part = os.path.relpath(root, source)
                    dir_part = '' if dir_part == '.' else dir_part + '/'
                    file_path = os.path.join(root, name)
                    self.upload_file(file_path, os.path.join(
                        destination, dir_part, name), overwrite)
        else:
            self.upload_file(source, destination, overwrite)

    def save(self, data: str):
        file_client = DataLakeFileClient(
            self.account_url, self.file_system_name, self.root_directory, self.credential)
        file_client.upload_data(data, overwrite=True)

    def upload_file(self, source: str, destination: str, overwrite: bool = True):
        if (not os.path.exists(source)):
            return
        file_read = open(source, "rb")
        file_data = file_read.read()

        file_client = self.file_system_client.get_file_client(destination)
        if not file_client.exists():
            file_client.create_file()
        elif overwrite == False:
            file_read.close()
            return

        file_client.upload_data(data=file_data,
                                overwrite=True,
                                length=len(file_data))
        file_client.flush_data(len(file_data))
        file_read.close()


# https://github.com/Azure/azure-sdk-for-python/blob/azure-storage-blob_12.8.1/sdk/storage/azure-storage-blob/samples/blob_samples_directory_interface.py
class CloudStorageClient:

    def __init__(self,
                 connection_string=None,
                 container_name=None,
                 container_url=None):
        if container_url == None:
            service_client = BlobServiceClient.from_connection_string(
                connection_string)
            self.client = service_client.get_container_client(container_name)
        else:
            parsed_sas = urlparse(container_url.rstrip('/'))
            if not '.blob.' in parsed_sas.hostname:
                raise ValueError("Not a valid blob sas url")
            self.client = ContainerClient.from_container_url(container_url)

    def upload(self, source, dest):
        '''
    Upload a file or directory to a path inside the container
    '''
        if (os.path.isdir(source)):
            self.upload_dir(source, dest)
        else:
            self.upload_file(source, dest)

    def upload_file(self, source, dest):
        '''
    Upload a single file to a path inside the container
    '''
        logging.debug(f'Uploading {source} to {dest}')
        with open(source, 'rb') as data:
            self.client.upload_blob(name=dest, data=data, overwrite=True)

    def upload_dir(self, source, dest):
        '''
    Upload a directory to a path inside the container
    '''
        prefix = '' if dest == '' else dest + '/'
        prefix += os.path.basename(source) + '/'
        for root, dirs, files in os.walk(source):
            for name in files:
                dir_part = os.path.relpath(root, source)
                dir_part = '' if dir_part == '.' else dir_part + '/'
                file_path = os.path.join(root, name)
                blob_path = prefix + dir_part + name
                self.upload_file(file_path, blob_path)

    def download(self, source, dest):
        '''
    Download a file or directory to a path on the local filesystem
    '''
        if not dest:
            raise Exception('A destination must be provided')

        blobs = self.ls_files(source, recursive=True)
        if blobs:
            # if source is a directory, dest must also be a directory
            if not source == '' and not source.endswith('/'):
                source += '/'
            if not dest.endswith('/'):
                dest += '/'
            # append the directory name from source to the destination
            dest += os.path.basename(os.path.normpath(source)) + '/'

            blobs = [source + blob for blob in blobs]
            for blob in blobs:
                blob_dest = dest + os.path.relpath(blob, source)
                self.download_file(blob, blob_dest)
        else:
            bc = self.client.get_blob_client(blob=source)
            if bc.exists():
                self.download_file(source, dest)
            else:
                return False
        return True

    def download_file(self, source, dest, existence_check=False):
        '''
    Download a single file to a path on the local filesystem
    '''
        # dest is a directory if ending with '/' or '.', otherwise it's a file
        if dest.endswith('.'):
            dest += '/'
        blob_dest = dest + os.path.basename(source) if dest.endswith(
            '/') else dest

        logging.debug(f'Downloading {source} to {blob_dest}')
        os.makedirs(os.path.dirname(blob_dest), exist_ok=True)
        bc = self.client.get_blob_client(blob=source)

        if existence_check == True:
            if bc.exists() == False:
                return

        with open(blob_dest, 'wb') as file:
            data = bc.download_blob()
            file.write(data.readall())

    def ls_files(self, path, recursive=False):
        '''
    List files under a path, optionally recursively
    '''
        if not path == '' and not path.endswith('/'):
            path += '/'

        blob_iter = self.client.list_blobs(name_starts_with=path)
        files = []
        for blob in blob_iter:
            relative_path = os.path.relpath(blob.name, path)
            if recursive or not '/' in relative_path:
                files.append(relative_path)
        return files

    def ls_dirs(self, path, recursive=False):
        '''
    List directories under a path, optionally recursively
    '''
        if not path == '' and not path.endswith('/'):
            path += '/'

        blob_iter = self.client.list_blobs(name_starts_with=path)
        dirs = []
        for blob in blob_iter:
            relative_dir = os.path.dirname(os.path.relpath(blob.name, path))
            if relative_dir and (recursive or not '/'
                                 in relative_dir) and not relative_dir in dirs:
                dirs.append(relative_dir)

        return dirs

    def rm(self, path, recursive=False):
        '''
    Remove a single file, or remove a path recursively
    '''
        if recursive:
            self.rmdir(path)
        else:
            logging.debug(f'Deleting {path}')
            self.client.delete_blob(path)

    def rmdir(self, path):
        '''
    Remove a directory and its contents recursively
    '''
        blobs = self.ls_files(path, recursive=True)
        if not blobs:
            return

        if not path == '' and not path.endswith('/'):
            path += '/'
        blobs = [path + blob for blob in blobs]
        logging.debug(f'Deleting {", ".join(blobs)}')
        self.client.delete_blobs(*blobs)
