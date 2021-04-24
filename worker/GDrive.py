import io
import re
import objects
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
standard_file_fields = 'files(id, name, parents, createdTime, modifiedTime)'


class Drive:
    def __init__(self, path):
        scope = ['https://www.googleapis.com/auth/drive']
        credentials = service_account.Credentials.from_service_account_file(path, scopes=scope)
        self.client = build('drive', 'v3', credentials=credentials)

    @staticmethod
    def revoke_time(file):
        for key in ['modifiedTime', 'createdTime']:
            if file.get(key):
                stamp = re.sub(r'\..*?Z', '', file[key])
                file[key] = objects.stamper(stamp, '%Y-%m-%dT%H:%M:%S')
        return file

    def delete_file(self, file_id):
        self.client.files().delete(fileId=file_id).execute()

    def add_file_to_folder(self, file_id, folder_id):
        self.client.files().update(fileId=file_id, addParents=folder_id).execute()

    def update_file(self, file_id, file_path, description=''):
        file_metadata = {'description': description}
        media_body = MediaFileUpload(file_path, resumable=True)
        return self.client.files().update(fileId=file_id, media_body=media_body, body=file_metadata).execute()

    def get_permissions(self, file_id):
        fields = 'permissions(id, emailAddress, role)'
        response = self.client.permissions().list(fileId=file_id, fields=fields).execute()
        return response['permissions']

    def file(self, file_id):
        fields = 'id, name, parents, createdTime, modifiedTime'
        result = self.client.files().get(fileId=file_id, fields=fields).execute()
        return self.revoke_time(result)

    def create_folder(self, name, folder_id):
        file_metadata = {'name': name, 'parents': [folder_id], 'mimeType': 'application/vnd.google-apps.folder'}
        result = self.client.files().create(body=file_metadata, fields='id, name, createdTime').execute()
        return self.revoke_time(result)

    def create_file(self, file_path, folder_id, same_file_name='True'):
        if same_file_name == 'True':
            same_file_name = re.sub('(.*)/', '', file_path)
        media_body = MediaFileUpload(file_path, resumable=True)
        file_metadata = {'name': same_file_name, 'parents': [folder_id]}
        return self.client.files().create(body=file_metadata, media_body=media_body, fields='id').execute()

    def move_file_to_folder(self, file_id, folder_id, remove_from_previous_folder=True):
        file = self.client.files().get(fileId=file_id, fields='parents').execute()
        previous_parents = ''
        if remove_from_previous_folder:
            previous_parents = ",".join(file.get('parents'))
        self.client.files().update(fileId=file_id, addParents=folder_id, removeParents=previous_parents).execute()

    def get_file_by_name(self, file_name, fields=standard_file_fields):
        response = None
        drive_response = self.client.files().list(pageSize=1000, fields=fields).execute()
        for file in drive_response['files']:
            if file_name == file['name']:
                response = self.revoke_time(file)
                break
        return response

    def download_file(self, file_id, file_path):
        done = False
        file = io.FileIO(file_path, 'wb')
        downloader = MediaIoBaseDownload(file, self.client.files().get_media(fileId=file_id))
        while done is False:
            try:
                status, done = downloader.next_chunk()
            except IndexError and Exception:
                done = False

    def files(self, fields=standard_file_fields, only_folders=None, name_startswith=None, parents=None):
        query = ''
        response = []
        if only_folders:
            query = "mimeType='application/vnd.google-apps.folder'"
        if name_startswith:
            if query:
                query += ' and '
            query += f"name contains '{name_startswith}'"
        if parents:
            if query:
                query += ' and '
            query += f"'{parents}' in parents"
        result = self.client.files().list(q=query, pageSize=1000, fields=fields).execute()
        for file in result['files']:
            response.append(self.revoke_time(file))
        return response
