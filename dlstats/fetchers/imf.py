# -*- coding: utf-8 -*-

from dlstats.fetchers._commons import Fetcher, Categories, Series, Datasets, Providers, CodeDict, ElasticIndex
import urllib
import xlrd
import csv
import codecs
from datetime import datetime
import pandas
import pprint
from collections import OrderedDict
from re import match
from time import sleep
import requests
from lxml import etree
import io
import re

class Downloader():
    
    headers = {
        'user-agent': 'dlstats - https://github.com/Widukind/dlstats'
    }
    
    def __init__(self, url=None, filename=None, store_filepath=None, 
                 timeout=None, max_retries=0, replace=True):
        self.url = url
        self.filename = filename
        self.store_filepath = store_filepath
        self.timeout = timeout
        self.max_retries = max_retries
        
        if not self.store_filepath:
            self.store_filepath = tempfile.mkdtemp()
        else:
            if not os.path.exists(self.store_filepath):
                os.makedirs(self.store_filepath, exist_ok=True)
        
        self.filepath = os.path.abspath(os.path.join(self.store_filepath, self.filename))
        
        #TODO: force_replace ?
        
        if os.path.exists(self.filepath) and not replace:
            raise Exception("filepath is already exist : %s" % self.filepath)
        
    def _download(self):
        
        #TODO: timeout
        #TODO: max_retries (self.max_retries)
        #TODO: analyse rate limit dans headers
        
        start = time.time()
        try:
            #TODO: Session ?
            response = requests.get(self.url, 
                                    timeout=self.timeout, 
                                    stream=True, 
                                    allow_redirects=True,
                                    verify=False, #ssl
                                    headers=self.headers)

            if not response.ok:
                msg = "download url[%s] - status_code[%s] - reason[%s]" % (self.url, 
                                                                           response.status_code, 
                                                                           response.reason)
                logger.error(msg)
                raise Exception(msg)
            
            with open(self.filepath,'wb') as f:
                for chunk in response.iter_content():
                    f.write(chunk)
                    #TODO: flush ?            
                
            #TODO: response.close() ?
            
        except requests.exceptions.ConnectionError as err:
            raise Exception("Connection Error")
        except requests.exceptions.ConnectTimeout as err:
            raise Exception("Connect Timeout")
        except requests.exceptions.ReadTimeout as err:
            raise Exception("Read Timeout")
        except Exception as err:
            raise Exception("Not captured exception : %s" % str(err))            

        end = time.time() - start
        logger.info("download file[%s] - END - time[%.3f seconds]" % (self.url, end))
    
    def get_filepath(self, force_replace=False):
        
        if os.path.exists(self.filepath) and force_replace:
            os.remove(self.filepath)
        
        if not os.path.exists(self.filepath):
            logger.info("not found file[%s] - download dataset url[%s]" % (self.filepath, self.url))
            self._download()
        else:
            logger.info("use local dataset file [%s]" % self.filepath)
        
        return self.filepath

class IMF(Fetcher):
    def __init__(self, db=None, es_client=None):
        super().__init__(provider_name='IMF',  db=db, es_client=es_client) 
        self.provider_name = 'IMF'
        self.provider = Providers(name=self.provider_name,
                          long_name=' International Monetary Fund ',
                          region='World',
                          website='http://www.imf.org/',
                          fetcher=self)
    def upsert_dataset(self, datasetCode):
        if datasetCode=='WEO':
            for u in self.weo_urls:
                self.upsert_weo_issue(u,datasetCode)
                self.update_metas(datasetCode)
        else:
            if datasetCode=='IFS': 
                u = 'http://data.imf.org/ifs'
                self.upsert_ifs_issue(u,datasetCode)
                self.update_metas(datasetCode)
                      
        
        
        
        #else:
           # raise Exception("This dataset is unknown" + dataCode)

    @property
    def weo_urls(self):
        """Procedure for fetching the list of links to the Excel files from the
        WEO database
        :returns: list --- list of links
        >>> l = get_weo_links()
        >>> print(l[:4])
        ['http://www.imf.org/external/pubs/ft/weo/2015/01/weodata/WEOApr2015all.xls', 'http://www.imf.org/external/pubs/ft/weo/2014/02/weodata/WEOOct2014all.xls', 'http://www.imf.org/external/pubs/ft/weo/2014/01/weodata/WEOApr2014all.xls', 'http://www.imf.org/external/pubs/ft/weo/2013/02/weodata/WEOOct2013all.xls']
        """

        #We hardcode these links because their formats are different.
        output = ['http://www.imf.org/external/pubs/ft/weo/2006/02/data/WEOSep2006all.xls',
                  'http://www.imf.org/external/pubs/ft/weo/2007/01/data/WEOApr2007all.xls',
                  'http://www.imf.org/external/pubs/ft/weo/2007/02/weodata/WEOOct2007all.xls']

        webpage = requests.get('http://www.imf.org/external/ns/cs.aspx?id=28')
        html = etree.HTML(webpage.text)
        hrefs = html.xpath("//div[@id = 'content-main']/h4/a['href']")
        links = [href.values() for href in hrefs]
        #The last links of the WEO webpage lead to data we dont want to pull.
        links = links[:-16]
        #These are other links we don't want.
        links.pop(-8)
        links.pop(-10)
        links = [link[0][:-10]+'download.aspx' for link in links]
        
        output = []

        for link in links:
            webpage = requests.get(link)
            html = etree.HTML(webpage.text)
            final_link = html.xpath("//div[@id = 'content']//table//a['href']")
            final_link = final_link[0].values()
            output.append(link[:-13]+final_link[0])

        # we need to handle the issue in chronological order
        return(sorted(output))
        
    def upsert_weo_issue(self,url,dataset_code):
        dataset = Datasets(self.provider_name,dataset_code,
                           fetcher=self)
        weo_data = WeoData(dataset,url)
        dataset.name = 'World Economic Outlook'
        dataset.doc_href = 'http://www.imf.org/external/ns/cs.aspx?id=28'
        dataset.last_update = weo_data.release_date
        dataset.attribute_list.update_entry('flags','e','Estimated')
        dataset.series.data_iterator = weo_data
        dataset.update_database()

    def upsert_ifs_issue(self,url,dataset_code):
        dataset = Datasets(self.provider_name,dataset_code,
                           fetcher=self)
        ifs_data = Ifs_Data(dataset,url)
        dataset.name = 'International Financial Statistics'
        dataset.doc_href = 'http://data.imf.org/ifs'
        dataset.last_update = ifs_data.release_date
        dataset.series.data_iterator = ifs_data
        dataset.update_database()    
   
    def upsert_categories(self):
        document = Categories(provider = self.provider_name, 
                            name = 'WEO' , 
                            categoryCode ='WEO',
                            children = None,
                            fetcher=self)
        return document.update_database()





class IFS_Data():
    def __init__(self,dataset,url=None, filename=None, store_filepath=None, is_autoload=True):
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        self.list_keys = []
        self.original_data = {}
        self.country_list= {}
        self.indicator_list= {}
        self.START_DATE_A = pandas.Period('1947','A').ordinal
        self.START_DATE_Q = pandas.Period('1947-4','Q').ordinal
        self.START_DATE_M = pandas.Period('1947-12','M').ordinal
    
        
    def make_url():
        pass
    def get_store_path(self):
        return self.store_filepath or os.path.abspath(os.path.join(
            tempfile.gettempdir(), 
            self.dataset.provider_name, 
            self.dataset.dataset_code))
    
    def _load_data(self, datas=None):
        
        kwargs = {}
        
        if not datas:
            store_filepath = self.get_store_path()
            # TODO: timeout, replace
            download = Downloader(url=self.url, filename=self.filename, store_filepath=store_filepath)
            
            filepath = extract_zip_file(download.get_filepath())
            _file = open(filepath)
        else:
            fileobj = io.StringIO(datas, newline="\n")
            _file = fileobj
    
        self.load_original_data(csvfile=_file)
        return
        
    def load_original_data(self,csvfile=None):        
#        with open('/home/salimeh/IFS/IFS_10-20-2015 20-09-38-08.csv') as csvfile:
            reader = csv.DictReader(csvfile)
            print(reader.fieldnames)
            datefmt = re.compile("(\d\d\d\d)([MQ]*)(\d*)")
            for row in reader :
                print(row)
                date1 = datefmt.match(row["Time Period"])
                year = int(date1.group(1))
                if date1.group(2) is '':
                    if len(date1.group(0)) > 4:
                        # there is a character other than M or Q after the year
                        logging.CRITICAL("unknown time format: " + row)
                    self.frequency = 'A'
                    offset = year - 1947
                else:
                    self.frequency = date1.group(2)
                    subperiod = int(date1.group(3))
                    if self.frequency == 'Q':
                        offset = 4*(year-1948) + subperiod
                    else:
                        offset = 12*(year-1948) + subperiod
                self.key = row["Indicator Code"]+'.'+row["Country Code"] +'.'+ self.frequency 
                if self.key in self.original_data.keys():
                    self.original_data[self.key]["values"][offset] = row["Value"]
                    self.original_data[self.key]["Status"][offset] = row["Status"]    
                    
                    if offset < self.original_data[self.key]["start_date"] :
                        self.original_data[self.key]["start_date"] = offset
                    elif offset  > self.original_data[self.key]["end_date"] :
                        self.original_data[self.key]["end_date"] = offset  
                else:
                    bson = {}
                    bson["start_date"] = offset
                    bson["end_date"] = offset
                    if self.frequency == 'A':
                        bson["values"] =  ["na" for i in range(2015-1947)]
                        bson["Status"] =  ["na" for i in range(2015-1947)]
                    elif self.frequency == 'M':                 
                        bson["values"] =  ["na" for i in range(12*(2016-1947))] # there is 2015M6 ..
                        bson["Status"] =  ["na" for i in range(12*(2016-1947))]
                    elif self.frequency == 'Q':
                        bson["values"] =  ["na" for i in range(4*(2016-1947))]  # there is 2015Q1 ..
                        bson["Status"] =  ["na" for i in range(4*(2016-1947))]
                    bson["Country Code"] = row["Country Code"]
                    bson["Indicator Code"] = row["Indicator Code"]
                    bson["frequency"] =self.frequency
                    bson["values"][offset] = row["Value"]
                    bson["Status"][offset] = row["Status"]
                    self.original_data[self.key] = bson
                if row["Country Code"] not in self.country_list.keys():
                    self.country_list[row["Country Code"]] =  row["Country Name"]               
                if row["Indicator Code"] not in self.indicator_list.keys():
                    self.indicator_list[row["Indicator Code"]] = row["Indicator Name"]   
            self.original_data_iterator = iter(self.original_data)        
            self.list_keys = list( self.original_data.keys())     
            self.release_date = datetime.strptime("06/11/15", "%d/%m/%y")

#    def load_original_data(self,csvfile=None):        
##        with open('/home/salimeh/IFS/IFS_10-20-2015 20-09-38-08.csv') as csvfile:
#        reader = csv.DictReader(csvfile)
#        for row in reader :
#            self.frequency = 'A'
#            plus_fre = 23    # Default base year in pandas.period is 1970, 1970-1947=23
#            if 'Q' in row["Time Period"]:
#                self.frequency = 'Q'
#                plus_fre = 23*4
#            if 'M' in row["Time Period"]: 
#                self.frequency = 'M' 
#                plus_fre = 23*12
#            self.key = row["Indicator Code"]+'.'+row["Country Code"] +'.'+ self.frequency 
#            if self.key in self.original_data.keys():
#                self.original_data[self.key]["Country Code"] = row["Country Code"]
#                self.original_data[self.key]["Indicator Code"] = row["Indicator Code"]
#                self.original_data[self.key]["frequency"] =self.frequency
#                self.original_data[self.key]["values"][pandas.Period(row["Time Period"].replace('M', '-'),self.frequency).ordinal+plus_fre-1] = row["Value"]
#                self.original_data[self.key]["Status"][pandas.Period(row["Time Period"].replace('M', '-'),self.frequency).ordinal+plus_fre-1] = row["Status"]    
#            else:
#                self.original_data[self.key] = {}
#                self.original_data[self.key]["values"] =  ["na" for i in range(2015-1947)]
#                self.original_data[self.key]["Status"] =  ["na" for i in range(2015-1947)]
#                if self.frequency == 'M':                 
#                    self.original_data[self.key]["values"] =  ["na" for i in range(12*(2016-1947))] # there is 2015M6 ..
#                    self.original_data[self.key]["Status"] =  ["na" for i in range(12*(2016-1947))]
#                if self.frequency == 'Q':
#                    self.original_data[self.key]["values"] =  ["na" for i in range(4*(2016-1947))]  # there is 2015Q1 ..
#                    self.original_data[self.key]["Status"] =  ["na" for i in range(4*(2016-1947))]
#                self.original_data[self.key]["Country Code"] = row["Country Code"]
#                self.original_data[self.key]["Indicator Code"] = row["Indicator Code"]
#                self.original_data[self.key]["frequency"] =self.frequency
#                self.original_data[self.key]["values"][pandas.Period(row["Time Period"].replace('M', '-'),self.frequency).ordinal+plus_fre-1] = row["Value"]
#                self.original_data[self.key]["Status"][pandas.Period(row["Time Period"].replace('M', '-'),self.frequency).ordinal+plus_fre-1] = row["Status"]
#            if row["Country Code"] in self.country_list.keys():
#                pass
#            else:
#                self.country_list[row["Country Code"]] =  row['\ufeff"Country Name"']               
#            if row["Indicator Code"] in self.indicatore_list.keys():
#                pass
#            else:    
#                self.indicatore_list[row["Indicator Code"]] = row["Indicator Name"]   
#        self.row_range = iter(range(len(self.original_data.keys())))        
#        self.list_keys = list( self.original_data.keys())     
#        self.release_date = datetime.strptime("06/11/15", "%d/%m/%y")

    def __next__(self):
        key = next(self.original_data_iterator)
        row = self.original_data[key]
        #row = next(self.sheet) 
        series = self.build_series(key,row)
        if series is None:
            raise StopIteration()            
        return(series)
    
    def build_series(self,key,row):
        series = {}
        dimensions = {}
        frequency = row['frequency']
        country_code = row['Country Code']
        country_name = self.country_list[country_code]
        indicator_code = row['Indicator Code']
        indicator_name = self.indicator_list[indicator_code]
        series_name = indicator_name + '.' + country_name
        dimensions['Country'] = self.dimension_list.update_entry('Country', country_code, country_name)
        dimensions['Indicator'] = self.dimension_list.update_entry('Indicator', indicator_code, indicator_name)
        series['provider'] = self.provider_name
        series['datasetCode'] = self.dataset_code
        series['name'] = series_name
        series['key'] = key
        series['values'] = row['values']
        series['attributes'] = {'status': row['Status']}
        series['dimensions'] = dimensions
        series['lastUpdate'] = self.release_date
        if frequency == 'A':
            series['startDate'] = self.START_DATE_A + row['start_date']
            series['endDate'] = self.START_DATE_A + row['end_date']
        elif frequency == 'Q':
            series['startDate'] = self.START_DATE_Q + row['start_date']
            series['endDate'] = self.START_DATE_Q + row['end_date']
        elif frequency == 'M':
            series['startDate'] = self.START_DATE_M + row['start_date']
            series['endDate'] = self.START_DATE_M + row['end_date']
        series['frequency'] = frequency
        return(series)


        
class WeoData():
    def __init__(self,dataset,url):
        self.provider_name = dataset.provider_name
        self.dataset_code = dataset.dataset_code
        self.dimension_list = dataset.dimension_list
        self.attribute_list = dataset.attribute_list
        datafile = urllib.request.urlopen(url).read().decode('latin-1').splitlines()
        self.sheet = csv.DictReader(datafile, delimiter='\t')
        self.years = self.sheet.fieldnames[9:-1]
        print(self.years)
        self.start_date = pandas.Period(self.years[0],freq='annual')
        self.end_date = pandas.Period(self.years[-1],freq='annual')
        self.release_date = datetime.strptime(match(".*WEO(\w{7})",url).groups()[0], "%b%Y")

    def __next__(self):
        row = next(self.sheet) 
        series = self.build_series(row)
        if series is None:
            raise StopIteration()            
        return(series)
        
    def build_series(self,row):
        if row['Country']:               
            series = {}
            values = []
            dimensions = {}
            for year in self.years:
                values.append(row[year])
            dimensions['Country'] = self.dimension_list.update_entry('Country', row['ISO'], row['Country'])
            dimensions['WEO Country Code'] = self.dimension_list.update_entry('WEO Country Code', row['WEO Country Code'], row['Country'])
            dimensions['Subject'] = self.dimension_list.update_entry('Subject', row['WEO Subject Code'], row['Subject Descriptor'])
            dimensions['Units'] = self.dimension_list.update_entry('Units', '', row['Units'])
            dimensions['Scale'] = self.dimension_list.update_entry('Scale', row['Scale'], row['Scale'])
            series_name = row['Subject Descriptor']+'.'+row['Country']+'.'+row['Units']
            series_key = row['WEO Subject Code']+'.'+row['ISO']+'.'+dimensions['Units']
            #release_dates = [ self.release_date for v in values]
            series['provider'] = self.provider_name
            series['datasetCode'] = self.dataset_code
            series['name'] = series_name
            series['key'] = series_key
            series['values'] = values
            series['attributes'] = {}
            if row['Estimates Start After']:
                estimation_start = int(row['Estimates Start After']);
                series['attributes'] = {'flag': [ '' if int(y) < estimation_start else 'e' for y in self.years]}
            series['dimensions'] = dimensions
            series['lastUpdate'] = self.release_date
            #series['releaseDates'] = release_dates
            series['startDate'] = self.start_date.ordinal
            series['endDate'] = self.end_date.ordinal
            series['frequency'] = 'A'
            if row['Subject Notes']:
                series['notes'] = row['Subject Notes']
            if row['Country/Series-specific Notes']:
                row['Country/Series-specific Notes'] += '\n' + row['Country/Series-specific Notes']
            return(series)
        else:
            return None
        
if __name__ == "__main__":
    w = IMF()
#    w.provider.update_database()
#    w.upsert_categories()
#    w.upsert_dataset('WEO') 
    w.upsert_dataset('IFS') 


              
