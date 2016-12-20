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

rootpath = '../2030-watch.de/_data/datasets/online/'
rootpathstatic = '../2030-watch.de/datastatic/datasets/online/'

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
        #Use this to selectively convert a file or leave one out
        if "SDG1_People_at_risk_of_poverty_EUROSTAT_2014" not in filedata['name']:
          continue
        if files_resource['mimeType'] == u"application/vnd.google-apps.spreadsheet":
          for format in ('text/csv', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/vnd.oasis.opendocument.spreadsheet'):
            files_resource = service.files().export(fileId=item['id'], mimeType=format, **param)
            resp, content = service._http.request(files_resource.uri)
            if resp.status == 200:
              print 'Got the file ' + filedata['name'] + ' as ' + format
              
              if format == 'text/csv':
                filedata['csv'] = content
                extension = '.csv'
                allcsvs.append(filedata)
              elif format == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
                extension = '.xlsx'
              elif format == 'application/vnd.oasis.opendocument.spreadsheet':
                extension = '.ods'
                
              namerootparts = filedata['name'].split('.')[0:-1]
              
              if len(namerootparts) == 0:
                filedata['nameroot'] = filedata['name'].replace(' ', '_') #Jekyll does this, so we do to
              else:
                filedata['nameroot'] = (''.join(namerootparts)).replace(' ', '_') #Jekyll does this, so we do to
              
              with file(rootpathstatic + filedata['nameroot'] + extension, 'wb') as outputfile:
                print 'Writing ' + rootpath + filedata['nameroot'] + extension
                outputfile.write(content)
            else:
              print 'An error occurred: %s getting %s' % (resp, filedata['name'])
        
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
    csvs = get_csv_data(service, "0BzOn_JyF6v0yRUNreHZ5RGRwS1E")
    #0B06K0pSAyW1gMi11dk1tdUp6Ylk")
    
    for csvdata in csvs:
        print 'Processing ' + csvdata['name']
        csvreader = csv.reader(StringIO.StringIO(csvdata['csv']))
        last_value = None
        tree = {}
                
        for row in csvreader:            
            this_value = row[0]
        
            if '$' in this_value: #Standard way of showing child elements, but not sub-child
                parts = this_value.split('$')
                root = parts[0]
                child = parts[-1]
                midchild = None
                if len(parts) == 3: #Happens in a couple of places
                    midchild = parts[1]
                
                if root not in tree:
                    tree[root] = {}
                if (midchild != None) and (midchild not in tree[root]):
                    tree[root][midchild] = {}
                        
                    
                if (len(row) > 1):
                    if (child in ('rating', 'other_relevant_SDGs', )) or (child == 'value' and root == 'source'):
                        rating_parts = row[1].split(',')
                        tree[root][child] = []
                        for rpart in rating_parts:
                            if rpart.strip() != "":
                                tree[root][child].append(getValueGeneric(rpart))
                    else:
                        if midchild == None:
                            #print root
                            #print child
                            #print row[1]
                            tree[root][child] = getValueGeneric(row[1])
                        else:
                            tree[root][midchild][child] = getValueGeneric(row[1])
            elif (len(row) > 1) and (row[1] != '') and (last_value != 'countries'):
                tree[this_value] = getValueGeneric(row[1])
                
            omitlastvalue = False
            if last_value == 'countries': #Although country names are child elements, they aren't formatted as such
                if 'scores' not in tree:
                    tree['scores'] = []
                    tree['scores'].append({'year': 'unknown'})
                    tree['scores'][0]['countries'] = []
                if row[0].strip() != "":
                    valueparts = row[1].split(' ')
                    # Note this script currently only copes with one year
                    tree['scores'][0]['countries'].append({'name': row[0], 'value': getValueGeneric(valueparts[0])}) #Remove things like range
                else:
                    try:
                        tree['scores'][0]['year'] = int(row[1]) #Should be a year
                    except:
                        pass #Stick with unknown
                    omitlastvalue = True #Here we have a line of years we want to ignore
            else: #Clamp last_value to countries
                if not omitlastvalue:
                    last_value = row[0]
                    
        if 'scores' in tree and 'scoring' in tree:
            tree['scoring']['scores'] = tree['scores']
            del tree['scores']
            
        if 'title_German' in tree:
            tree['title'] = tree['title_German']
            del tree['title_German']
            
        if 'maintainer' in tree['source']:
            tree['sponsor'] = tree['source']['maintainer']
            del tree['source']['maintainer']
            
        jsonfilename = rootpath + csvdata['nameroot'].replace(' ', '_') + '.json' #Conform to Jekyll's renaming of the filename
        with open(jsonfilename, 'wb') as outfile:
            print 'Writing ' + jsonfilename
            json.dump(tree, outfile, sort_keys=True, indent=4)
    

if __name__ == '__main__':
    main()

