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
      
      for item in items:
        filedata = {}
        files_resource = service.files().get(fileId=item['id']).execute()
        filedata['name'] = files_resource['title']
        if files_resource['mimeType'] == u"application/vnd.google-apps.spreadsheet":
          files_resource = service.files().export(fileId=item['id'], mimeType="text/csv", **param)
          resp, content = service._http.request(files_resource.uri)
          if resp.status == 200:
            print 'Got ' + filedata['name']
            filedata['csv'] = content
            allcsvs.append(filedata)
          else:
            print 'An error occurred: %s getting %s - retrying in 5s' % (resp, filedata['name'])
            sleep(5)
            resp, content = service._http.request(files_resource.uri)
            if resp.status == 200:
              print 'Got ' + filedata['name'] + " on 2nd attempt"
              filedata['csv'] = content
              allcsvs.append(filedata)
            else:
              print 'An error occurred: %s getting %s - giving up' % (resp, filedata['name'])
          
        
      page_token = children.get('nextPageToken')
      if page_token == None:
        print "Finished getting CSVs from Google Drive"
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
    csvs = get_csv_data(service, "0B06K0pSAyW1gMi11dk1tdUp6Ylk")
    
    for csvdata in csvs:
        csvreader = csv.reader(StringIO.StringIO(csvdata['csv']))
        last_value = None
        tree = {}
                
        for row in csvreader:
            if row[0] == '': continue
            
            this_value = row[0]
        
            if '$' in this_value: #Standard way of showing child elements, but not sub-child
                parts = this_value.split('$')
                root = parts[0]
                child = parts[1]
                
                if root not in tree:
                    tree[root] = {}
                    
                if (len(row) > 1):
                    tree[root][child] = getValueGeneric(row[1])
            elif (len(row) > 1) and (row[1] != '') and (last_value != 'countries'):
                tree[this_value] = row[1]
                
            omitlastvalue = False
            if last_value == 'countries': #Although country names are child elements, they aren't formatted as such
                if 'countries' not in tree:
                    tree['countries'] = []
                if row[0].strip() != "":
                    valueparts = row[1].split(' ')
                    tree['countries'].append({'name': row[0], 'value': getValueGeneric(valueparts[0])}) #Remove things like range
                else:
                    omitlastvalue = True #Here we have a line of years we want to ignore
            else: #Clamp last_value to countries
                if not omitlastvalue:
                    last_value = row[0]
                    
        if 'countries' in tree and 'scoring' in tree:
            tree['scoring']['countries'] = tree['countries']
            del tree['countries']
            
        with open('../2030-watch.de/_data/datasets/online/' + csvdata['name'][0:-4] + '.json', 'wb') as outfile:
            json.dump(tree, outfile, sort_keys=True, indent=4)
    

if __name__ == '__main__':
    main()

