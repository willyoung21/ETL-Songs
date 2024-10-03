from __future__ import print_function
import os.path
import pickle
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Definir el alcance (en este caso, para Google Drive)
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def authenticate_gdrive():
    """Realiza la autenticación con Google Drive API."""
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials/client_secret.json', SCOPES)
            creds = flow.run_local_server(port=0)

        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)

def upload_to_gdrive(file_path, file_name):
    """Sube un archivo a Google Drive."""
    service = authenticate_gdrive()
    file_metadata = {'name': file_name}
    media = MediaFileUpload(file_path, mimetype='text/csv')
    file = service.files().create(body=file_metadata,
                                    media_body=media,
                                    fields='id').execute()
    print(f'Archivo subido correctamente con ID: {file.get("id")}')

if __name__ == "__main__":
    file_path = os.path.join('data', 'processed', 'final_merged_spotify_grammys.csv')
    file_name = 'final_merged_spotify_grammys.csv'
    upload_to_gdrive(file_path, file_name)
