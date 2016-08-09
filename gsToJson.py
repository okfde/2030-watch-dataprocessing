import unicodecsv as csv
import json
import urllib

csvreader = csv.reader(urllib.urlopen('https://docs.google.com/spreadsheets/d/17yh8YFLWNkWskYIKn_96Y7bWeBFaiNj8o6pRexVHbFM/pub?output=csv'))

last_value = None

tree = {}

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
        
for row in csvreader:
    if row[0] == '': continue
    
    this_value = row[0]

    if '$' in this_value: #Standard way of showing child elements, but not sub-child
        parts = this_value.split('$')
        root = parts[0]
        child = parts[1]
        
        if root not in tree:
            tree[root] = {}
            
        tree[root][child] = getValueGeneric(row[1])
    elif row[1] != '' and last_value != 'countries':
        tree[this_value] = row[1]
        
    if last_value == 'countries': #Although country names are child elements, they aren't formatted as such
        if 'countries' not in tree:
            tree['countries'] = []
        valueparts = row[1].split(' ')
        tree['countries'].append({'name': row[0], 'value': getValueGeneric(valueparts[0])}) #Remove things like range
    else: #Clamp last_value to countries
        last_value = row[0]
    
#Fix sub-children
if 'source' in tree and 'scoring' in tree:
    tree['scoring']['source'] = tree['source']
    del tree['source']
if 'countries' in tree and 'scoring' in tree:
    tree['scoring']['countries'] = tree['countries']
    del tree['countries']
    
newScoring = []
newScoring.append(tree['scoring']);
tree['scoring'] = newScoring;
    
    
with open('../2030-watch-datasets/online/C_prevalence_of_obesity_WHO_2014.json', 'wb') as outfile:
    json.dump(tree, outfile, sort_keys=True, indent=4)