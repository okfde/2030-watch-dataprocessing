import httplib2
import os
import unicodecsv as csv
import json
import StringIO

from apiclient import discovery, errors
import oauth2client
from oauth2client import client
from oauth2client import tools
from time import sleep

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Drive API Python Quickstart'

def getValueGeneric(stringvalue):
    try:
        val = int(stringvalue)
        return val
    except ValueError:
        try:
            val = float(stringvalue)
            return val
        except ValueError:
            val = stringvalue
            return val

def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'drive-python-quickstart.json')

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def get_csv_data(service, folder_id):
  """Print files belonging to a folder.

  Args:
    service: Drive API service instance.
    folder_id: ID of the folder to print files from.
  """
  page_token = None
  while True:
    try:
      param = {}
      if page_token:
        param['pageToken'] = page_token

      children = service.children().list(
          folderId=folder_id, **param).execute()

      items = children.get('items', [])
      
      allcsvs = []
      
      counter = 0
      
      for item in items:
        filedata = {}
        files_resource = service.files().get(fileId=item['id']).execute()
        filedata['name'] = files_resource['title']
        counter += 1
        if files_resource['mimeType'] == u"text/csv":
          print 'Updating ' + str(counter) + ' of ' + str(len(items))
          body = {'mimeType': 'application/vnd.google-apps.spreadsheet'}
          files_resource = service.files().copy(fileId=item['id'],body=body, convert=True, **param).execute()
        else:
          print 'Skipping ' + str(counter) + ' of ' + str(len(items)) + '- not a CSV file'
        
      page_token = children.get('nextPageToken')
      if page_token == None:
        print "Finished converting CSVs to GS in Google Drive folder"
        return allcsvs
    except errors.HttpError, error:
      break
    
    

def main():
    """Shows basic usage of the Google Drive API.

    Creates a Google Drive API service object and outputs the names and IDs
    for up to 10 files.
    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v2', http=http)
    get_csv_data(service, "0B06K0pSAyW1gMi11dk1tdUp6Ylk")

    

if __name__ == '__main__':
    main()

