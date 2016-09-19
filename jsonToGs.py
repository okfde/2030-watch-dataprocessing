import unicodecsv as csv
import json
import urllib
import os
from collections import OrderedDict
from jsonmerge import merge

#http://stackoverflow.com/questions/30539679/python-read-several-json-files-from-a-folder
path_to_json = '../2030-watch.de/_data/datasets/online/'
json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

schema = {
    u'sponsor': u'',
    u'title': u'',
    u'short_indicator_description': {
        u'de': u'',
        u'en': u''
    },
    u'long_indicator_description': {
        u'de': u'',
        u'en': u'',
        u'baseunit': u'',
    },
    u'original_indicator_code': u'',
    u'original_title': u'',
    u'sdg': u'',
    u'target': {
        u'type': u'',
        u'value': u'',
        u'rating': [],
        u'explanation': {
            u'de': u'',
            u'en': u'',
        },
        u'target_reference': u'',
        u'tags': [],
        u'ministerial_responsibility': u'',
        u'target': u''
    },
    u'source': {
        u'link': u'',
        u'note': u'',
        u'publisher': u'',
        u'type': u'',
        u'value': u''
    },
    u'scoring': [
        {
            u'countries': [],
            u'maintainer': u'',
            u'timestamp': u'',
            u'timestamp_data_host': u'',
            u'type': u''
        }
    ]
}

for filename in json_files:
    
    #if filename != "D_Governance_Financial_Secrecy_Index_TaxJusticeNetwork_2015.json": continue
    
    newname = filename.split('.')[0] + '.csv'
    
    #print 'Converting ' + filename + ' to ' + newname
    
    jsondata = json.load(open(path_to_json + filename, 'rb'))
    
    if 'timestamp_data_host' in jsondata['scoring'][0]:
        new_timestamp = jsondata['scoring'][0]['timestamp_data_host']
    else:
        new_timestamp = ""
       
    if 'timestamp' in jsondata['scoring'][0]:
        new_timestamp_data_host = jsondata['scoring'][0]['timestamp']
    else:
        new_timestamp_data_host = ""
        
    #Swap
    jsondata['scoring'][0]['timestamp'] = new_timestamp
    jsondata['scoring'][0]['timestamp_data_host'] = new_timestamp_data_host
    
    if ('source' in jsondata['scoring'][0]):
        jsondata['source'] = jsondata['scoring'][0]['source']
        del jsondata['scoring'][0]['source']
        
    if ('baseunit' in jsondata):
        jsondata['long_indicator_description']['baseunit'] = jsondata['baseunit']
        del jsondata['baseunit']
        
    if ('sdg' in jsondata['target']):
        jsondata['sdg'] = jsondata['target']['sdg']
        del jsondata['target']['sdg']
        
    if type(jsondata['sdg']) == list:
        jsondata['sdg'] = jsondata['sdg'][0]
            
    if ('tags' in jsondata):
        jsondata['target']['tags'] = jsondata['tags']
        del jsondata['tags']
        
    if ('ministerial_responsibility' in jsondata):
        jsondata['target']['ministerial_responsibility'] = jsondata['ministerial_responsibility'][0]
        del jsondata['ministerial_responsibility']
    
    jsondata_uo = merge(schema, jsondata)
    
    jsondata = OrderedDict()
    
    for copykey in ('sponsor', 'original_title', 'original_indicator_code', 'title', 'sdg', 'short_indicator_description', 'long_indicator_description', 'target', 'scoring', 'source'):
        jsondata[copykey] = jsondata_uo[copykey]

    csvoutfile = open(path_to_json + newname, 'wb')

    csvwriter = csv.writer(csvoutfile)
    
    rows = []
    
    
        
    #TODO: add in optional things if not there, see metadata
    
   # #TODO:
    #Converting D_Reporting_and_Public_Disclosure_MPT_Data_2014.json to D_Reporting_and_Public_Disclosure_MPT_Data_2014.csv
#Bad key at scoring, type type <type 'list'>
 
    countries = []
    
    for key in jsondata:
        if key == 'scoring':
            csvwriter.writerow(['scoring',])
            actualdict = jsondata[key][0] #wrapped in array
            for scoringkey in actualdict:
                
                if scoringkey == 'countries':
                    countries = actualdict[scoringkey]
                    
                
                elif type(actualdict[scoringkey]) is dict:
                    csvwriter.writerow(['scoring$' + scoringkey,])
                    lastkey = scoringkey
                    for subscoringkey in actualdict[scoringkey]:
                        if type(actualdict[scoringkey][subscoringkey]) is unicode:
                            csvwriter.writerow(['scoring$' + scoringkey +'$' + subscoringkey, actualdict[scoringkey][subscoringkey]])
                        else:
                            print "Bad key at scoring, " + scoringkey + ', ' + subscoringkey + ' type ' + str(type(actualdict[scoringkey][subscoringkey]))
                    
                elif type(actualdict[scoringkey]) in (unicode, int):
                    csvwriter.writerow(['scoring$' + scoringkey, actualdict[scoringkey]])
                else:
                    print "Bad key at scoring, " + scoringkey + ' type ' + str(type(actualdict[scoringkey]))
            
        elif type(jsondata[key]) is dict:
            csvwriter.writerow([key,])
            topkey = key
            for subkey in jsondata[key]:
                if subkey == 'rating':
                    ratingsastext = []
                    for rating in jsondata[key][subkey]:
                        ratingsastext.append(str(rating))
                    ratingstext =  (',').join(ratingsastext)
                    csvwriter.writerow([key+'$'+subkey, ratingstext])
                elif subkey == 'tags':
                    #print jsondata[key][subkey]
                    if type(jsondata[key][subkey]) == list:
                        tagsastext =  (',').join(jsondata[key][subkey])
                    else:
                        tagsastext = jsondata[key][subkey]
                    csvwriter.writerow([key+'$'+subkey, tagsastext])
                    #print "converted to " + tagsastext
                elif subkey == 'explanation':
                    if type(jsondata[key][subkey]) == dict:
                        if 'de' in jsondata[key][subkey]:
                            csvwriter.writerow([key+'$'+subkey+'$de', jsondata[key][subkey]['de']])
                        if 'en' in jsondata[key][subkey]:
                            csvwriter.writerow([key+'$'+subkey+'$en', jsondata[key][subkey]['en']])
                    elif type(jsondata[key][subkey]) == unicode:
                        csvwriter.writerow([key+'$'+subkey+'$de', jsondata[key][subkey]])
                    else:
                        print "Bad key at " + key + ', ' + subkey
                elif type(jsondata[key][subkey]) in (unicode, int):
                    csvwriter.writerow([key+'$'+subkey, jsondata[key][subkey]])
                else:
                    print "Bad key at " + key + ', ' + subkey + ' with value ' + str(jsondata[key][subkey])
            
        elif type(jsondata[key]) in (unicode, int):
            csvwriter.writerow([key, jsondata[key]])
            
        else:
            print "Bad key at " + key + '  type ' + str(type(jsondata[key]))
            
    #Do countries last
    csvwriter.writerow(['countries',])
    
    year = [""]
    otheryears = {2011: "", 2012: "", 2013: "", 2014: "", 2015: "", 2016: "", 2017: ""}
    timestampvalue = jsondata['scoring'][0]['timestamp_data_host']
    if type(timestampvalue) == int: #just year
        year.append(timestampvalue)
        del otheryears[year[1]]
    elif timestampvalue.strip() != "": #assume string
        try:
            year.append(int(timestampvalue[0:4])) #remove possible month/day
            del otheryears[year[1]] #this one will be first
        except:
            year.append("Unreadable year: " + timestampvalue)
    else:
        year.append("No year provided")
        
    #print str(year).split(",")[1] + " " + filename.split('.')[0]
        
    year.extend(sorted(otheryears.keys()))
        
    csvwriter.writerow(year)
    
    sortedcountries = sorted(countries, key=lambda t: t['name'])
    
    for country in sortedcountries:
         csvwriter.writerow([country['name'], country['value']])

