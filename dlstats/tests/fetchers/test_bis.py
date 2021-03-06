# -*- coding: utf-8 -*-

import zipfile
import io
import tempfile
import datetime
import os
from pprint import pprint

from dlstats.fetchers._commons import Datasets
from dlstats.fetchers import bis
from dlstats import constants

import unittest
from unittest import mock
import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR, BaseTestCase, BaseDBTestCase

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "bis"))

DATASETS = bis.DATASETS

# Nombre de série dans les exemples
SERIES_COUNT = 1

#---Dataset LBS-DISS
DATASETS['LBS-DISS']["dimensions_count"] = 12
DATASETS['LBS-DISS']['datas'] = """Dataset,"Locational Banking Statistics - disseminated data"
Retrieved on,Wed Sep 16 08:13:35 GMT 2015
Subject,"BIS locational banking"
"Frequency","Quarterly"
"Decimals","Three"
"Unit of measure","US Dollar"
"Unit Multiplier","Millions"
"Frequency","Measure","Balance sheet position","Type of instruments","Currency denomination","Currency type of reporting country","Parent country","Type of reporting institutions","Reporting country","Counterparty sector","Counterparty country","Position type","Time Period","1977-Q4","2015-Q1"
"Q:Quarterly","F:FX and break adjusted change (BIS calculated)","C:Total claims","A:All instruments","CHF:Swiss Franc","A:All currencies (=D+F+U)","5J:All countries","A:All reporting banks/institutions (domestic, foreign, consortium and unclassified)","5A:All reporting countries","A:All sectors","1C:International organisations","N:Cross-border","Q:F:C:A:CHF:A:5J:A:5A:A:1C:N","NaN","419.158"
"""

#---Dataset CBS
DATASETS['CBS']["dimensions_count"] = 11
DATASETS['CBS']['datas'] = """Dataset,"Consolidated Banking Statistics"
Retrieved on,Wed Sep 16 09:13:14 GMT 2015
Subject,"BIS consolidated banking"
"Frequency","Quarterly"
"Decimals","Three"
"Unit of measure","US Dollar"
"Unit Multiplier","Millions"
"Measure","Amounts outstanding / Stocks"
"Frequency","Measure","Reporting country","CBS bank type","CBS reporting basis","Balance sheet position","Type of instruments","Remaining maturity","Currency type of booking location","Counterparty sector","Counterparty country","Time Period","1983-Q4","2015-Q1"
"Q:Quarterly","S:Amounts outstanding / Stocks","5A:All reporting countries","4B:Domestic banks","F:Immediate counterparty basis","B:Local claims","A:All instruments","A:All maturities","LC1:Local currency","A:All sectors","1C:International organisations","Q:S:5A:4B:F:B:A:A:LC1:A:1C","","1986.2"
"""

#---Dataset DSS
DATASETS['DSS']["dimensions_count"] = 15
DATASETS['DSS']['datas'] = """Dataset,"Debt securities statistics"
Retrieved on,Wed Sep 16 07:35:48 GMT 2015
Subject,"BIS Debt securities"
"Issue type","All issue types"
"Collateral type (for future expansion)","All issues"
"Frequency","Quarterly"
"Default risk (for future expansion)","All credit ratings"
"Decimals","Zero"
"Unit of measure","US Dollar"
"Unit Multiplier","Millions"
"Frequency","Issuer residence","Issuer nationality","Issuer sector - immediate borrower","Issuer sector - ultimate borrower","Issue market","Issue type","Issue currency group","Issue currency","Original maturity","Remaining maturity","Rate type","Default risk (for future expansion)","Collateral type (for future expansion)","Measure","Time Period","1962-Q4","2015-Q2"
"Q:Quarterly","1C:International organisations","3P:All countries excluding residents","1:All issuers","1:All issuers","C:International markets","A:All issue types","A:All currencies","EU1:Sum of ECU, Euro and legacy currencies now included in the Euro","A:All maturities","A:All maturities","A:All rate types","A:All credit ratings","A:All issues","C:Gross issues","Q:1C:3P:1:1:C:A:A:EU1:A:A:A:A:A:C","","17041"
"""

#---Dataset CNFS
DATASETS['CNFS']["dimensions_count"] = 7
DATASETS['CNFS']['datas'] = """Dataset,"BIS long series on total credit"
Retrieved on,Wed Sep 16 09:34:20 GMT 2015
Subject,"BIS long series on total credit"
"Frequency","Quarterly"
"Collection Indicator","End of period"
"Frequency","Borrowers' country","Borrowing sector","Lending sector","Valuation","Unit type","Type of adjustment","Time Period","1940-Q2","2015-Q1"
"Q:Quarterly","AR:Argentina","C:Non financial sector","A:All sectors","M:Market value","770:Percentage of GDP","A:Adjusted for breaks","Q:AR:C:A:M:770:A","","57.4"
"""

#---Dataset DSRP
DATASETS['DSRP']["dimensions_count"] = 3
DATASETS['DSRP']['datas'] = """Dataset,"BIS Debt service ratio"
Retrieved on,Wed Sep 16 08:47:38 GMT 2015
Subject,"BIS debt service ratio"
"Frequency","Quarterly"
"Collection Indicator","End of period"
"Unit of measure","Per Cent"
"Unit Multiplier","Units"
"Frequency","Borrowers' country","Borrowers","Time Period","1999-Q1","2015-Q1"
"Q:Quarterly","AU:Australia","H:Households & NPISHs","Q:AU:H","10","15.3"
"""

#---Dataset PP-SS
DATASETS['PP-SS']["dimensions_count"] = 4
DATASETS['PP-SS']['datas'] = """Dataset,"BIS Selected property prices"
Retrieved on,Wed Sep 16 09:10:57 GMT 2015
Subject,"BIS property prices: selected series"
"Frequency","Quarterly"
"Unit Multiplier","Units"
"Frequency","Reference area","Value","Unit of measure","Time Period","1966-Q1","2015-Q2"
"Q:Quarterly","AT:Austria","N:Nominal","628:Index, 2010 = 100","Q:AT:N:628","",""
"""

#---Dataset PP-LS
DATASETS['PP-LS']["dimensions_count"] = 2
DATASETS['PP-LS']['datas'] = """Dataset,"BIS Long property prices"
Retrieved on,Wed Sep 16 09:11:12 GMT 2015
Subject,"BIS property prices: long series"
"Frequency","Quarterly"
"Unit of measure","Index, 1995 = 100"
"Unit Multiplier","Units"
"Frequency","Reference area","Time Period","1970-Q1","2015-Q2"
"Q:Quarterly","AU:Australia","Q:AU","9.84",""
"""

#---Dataset EERI
DATASETS['EERI']["dimensions_count"] = 4
DATASETS['EERI']['datas'] = """Dataset,"BIS Effective Exchange Rates"
Retrieved on,Thu Oct 15 12:56:58 GMT 2015
Subject,"BIS effective exchange rates"
"Frequency","Monthly"
"Frequency","Type","Basket","Reference area","Time Period","1964-01","2015-09"
"M:Monthly","N:Nominal","B:Broad (61 economies)","AE:United Arab Emirates","M:N:B:AE","","119.52"
"""

#---AGENDA
AGENDA_FP = os.path.abspath(os.path.join(RESOURCES_DIR, 'agenda.html'))

def mock_get_store_path(self):
    return os.path.abspath(os.path.join(tempfile.gettempdir(), 
                                        self.dataset.provider_name, 
                                        self.dataset.dataset_code,
                                        "tests"))

def write_zip_file(zip_filepath, filename, txt):
    """Create file in zipfile
    """
    with zipfile.ZipFile(zip_filepath, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename, txt)
        
def get_filepath(dataset_code):
    """Create CSV file in zipfile
    
    Return local filepath of zipfile
    """
    dataset = DATASETS[dataset_code]
    zip_filename = dataset['filename']
    filename = zip_filename.replace(".zip", ".csv")
    dirpath = os.path.join(tempfile.gettempdir(), bis.PROVIDER_NAME, dataset_code, "tests")
    filepath = os.path.abspath(os.path.join(dirpath, zip_filename))
    
    if os.path.exists(filepath):
        os.remove(filepath)
        
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    
    write_zip_file(filepath, filename, DATASETS[dataset_code]['datas'])
    
    return filepath

def mock_get_filepath(self):
    """Patch for not remove existing file
    """
    if not os.path.exists(self.filepath):
        self._download()
    return self.filepath

def mock_streaming(filepath):
    '''body for large file'''
    with open(filepath, 'rb') as fp:
        for line in fp:
            yield line        
            
    
def load_fake_datas(select_dataset_code=None):
    """Load datas from DATASETS dict
    
    key: DATASETS[dataset_code]['datas']
    """
    
    fetcher = bis.BIS()
    
    results = {}
    
    for dataset_code, dataset in DATASETS.items():
        
        if select_dataset_code and select_dataset_code != dataset_code:
            continue
        
        _dataset = Datasets(provider_name=bis.PROVIDER_NAME, 
                    dataset_code=dataset_code, 
                    name=dataset['name'], 
                    doc_href=dataset['doc_href'], 
                    fetcher=fetcher, 
                    is_load_previous_version=False)
        
        dataset_datas = bis.BIS_Data(_dataset, is_autoload=False)
        dataset_datas._load_datas(dataset['datas'])
        
        results[dataset_code] = {'series': []}

        for d in dataset_datas.rows:
            row = bis.csv_dict(dataset_datas.headers, d)
            results[dataset_code]['series'].append(dataset_datas.build_serie(d))
            
            #pprint(results)
    return results

def get_agenda():
    with open(AGENDA_FP) as fh:
        page = fh.read()
    return page

class BISUtilsTestCase(BaseTestCase):
    """BIS Utils
    """

    def test_load_read_csv(self):
        """Load special csv - direct from string
        """
        # nosetests -s -v dlstats.tests.fetchers.test_bis:BISUtilsTestCase.test_load_read_csv

        d = {}
        #d['CNFS'] = DATASETS['CNFS'].copy()        
        d = DATASETS.copy()
        
        for dataset_code, dataset in d.items():
            datas = dataset['datas']            
            fileobj = io.StringIO(datas, newline="\n")
            rows, headers, release_date, dimension_keys, periods = bis.local_read_csv(fileobj=fileobj)
            #len(dimension_keys)
            #print(headers)
            self.assertTrue('KEY' in headers)
            
            line1 = bis.csv_dict(headers, next(rows))
            #TODO: test values ?
            #pprint(line1)
        
class BISDatasetsDBTestCase(BaseDBTestCase):
    """Fetchers Tests - with DB
    
    sources from DATASETS[dataset_code]['datas'] written in zip file
    """
    
    # nosetests -s -v dlstats.tests.fetchers.test_bis:BISDatasetsDBTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)
        
        self.fetcher = bis.BIS(db=self.db)
        self.dataset_code = None
        self.dataset = None        
        self.filepath = None
        
    @httpretty.activate
    @mock.patch('dlstats.fetchers.bis.Downloader.get_filepath', mock_get_filepath)
    def _common_tests(self):
        
        self._collections_is_empty()
        
        url = DATASETS[self.dataset_code]['url']

        self.filepath = get_filepath(self.dataset_code)
        self.assertTrue(os.path.exists(self.filepath))
        
        httpretty.register_uri(httpretty.GET, 
                               url,
                               body=mock_streaming(self.filepath),
                               status=200,
                               content_type='application/octet-stream;charset=UTF-8',
                               streaming=True)
        
        # provider.update_database
        self.fetcher.provider.update_database()
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": self.fetcher.provider_name})
        self.assertIsNotNone(provider)
        
        # upsert_data_tree
        self.fetcher.upsert_data_tree()
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": self.fetcher.provider_name}) 
        self.assertIsNotNone(provider['data_tree'])
        
        dataset = Datasets(provider_name=self.fetcher.provider_name, 
                           dataset_code=self.dataset_code, 
                           name=DATASETS[self.dataset_code]['name'], 
                           doc_href=DATASETS[self.dataset_code]['doc_href'], 
                           fetcher=self.fetcher)

        fetcher_data = bis.BIS_Data(dataset,
                                    url=url, 
                                    filename=DATASETS[self.dataset_code]['filename'],
                                    store_filepath=os.path.dirname(self.filepath))
        
        dataset.series.data_iterator = fetcher_data
        dataset.update_database()

        self.dataset = self.db[constants.COL_DATASETS].find_one({'provider_name': self.fetcher.provider_name, 
                                                            "dataset_code": self.dataset_code})
        
        self.assertIsNotNone(self.dataset)
        
        self.assertEqual(len(self.dataset["dimension_list"]), DATASETS[self.dataset_code]["dimensions_count"])
        
        series = self.db[constants.COL_SERIES].find({'provider_name': self.fetcher.provider_name, 
                                                     "dataset_code": self.dataset_code})
        self.assertEqual(series.count(), SERIES_COUNT)
        
        
    def test_lbs_diss(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:BISDatasetsDBTestCase.test_lbs_diss
                
        self.dataset_code = 'LBS-DISS'
        
        self._common_tests()        

        serie = self.db[constants.COL_SERIES].find_one({'provider_name': self.fetcher.provider_name, 
                                                        "dataset_code": self.dataset_code,
                                                        "key": "Q:F:C:A:CHF:A:5J:A:5A:A:1C:N"})
        self.assertIsNotNone(serie)
        
        d = serie['dimensions']
        self.assertEqual(d["Frequency"], 'Q')
        self.assertEqual(d["Measure"], 'F')
        self.assertEqual(d["Balance sheet position"], 'C')
        
        self.assertEqual(serie["name"], 'Quarterly - FX and break adjusted change (BIS calculated) - Total claims - All instruments - Swiss Franc - All currencies (=D+F+U) - All countries - All reporting banks/institutions (domestic, foreign, consortium and unclassified) - All reporting countries - All sectors - International organisations - Cross-border')
        
        #TODO: meta_datas tests  

        #TODO: clean filepath

    def test_cbs(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:BISDatasetsDBTestCase.test_cbs
                
        self.dataset_code = 'CBS'
        
        self._common_tests()        

        serie = self.db[constants.COL_SERIES].find_one({'provider_name': self.fetcher.provider_name, 
                                                        "dataset_code": self.dataset_code,
                                                        "key": "Q:S:5A:4B:F:B:A:A:LC1:A:1C"})
        self.assertIsNotNone(serie)
        
    def test_dss(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:BISDatasetsDBTestCase.test_dss
                
        self.dataset_code = 'DSS'
        
        self._common_tests()        

        serie = self.db[constants.COL_SERIES].find_one({'provider_name': self.fetcher.provider_name, 
                                                        "dataset_code": self.dataset_code,
                                                        "key": "Q:1C:3P:1:1:C:A:A:EU1:A:A:A:A:A:C"})
        self.assertIsNotNone(serie)

    def test_cnfs(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:BISDatasetsDBTestCase.test_cnfs
        
        self.dataset_code = 'CNFS'        

        self._common_tests()

        serie = self.db[constants.COL_SERIES].find_one({'provider_name': self.fetcher.provider_name, 
                                                        "dataset_code": self.dataset_code,
                                                        "key": "Q:AR:C:A:M:770:A"})
        self.assertIsNotNone(serie)
        
        d = serie['dimensions']
        self.assertEqual(d["Frequency"], 'Q')
        self.assertEqual(d["Borrowing sector"], 'C')
        
        #TODO: meta_datas tests  
        #TODO: clean filepath

    def test_dsrp(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:BISDatasetsDBTestCase.test_dsrp
                
        self.dataset_code = 'DSRP'
        
        self._common_tests()        

        serie = self.db[constants.COL_SERIES].find_one({'provider_name': self.fetcher.provider_name, 
                                                        "dataset_code": self.dataset_code,
                                                        "key": "Q:AU:H"})
        self.assertIsNotNone(serie)
        
    def test_pp_ss(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:BISDatasetsDBTestCase.test_pp_ss
                
        self.dataset_code = 'PP-SS'
        
        self._common_tests()        

        serie = self.db[constants.COL_SERIES].find_one({'provider_name': self.fetcher.provider_name, 
                                                        "dataset_code": self.dataset_code,
                                                        "key": "Q:AT:N:628"})
        self.assertIsNotNone(serie)
        
    def test_pp_ls(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:BISDatasetsDBTestCase.test_pp_ls
                
        self.dataset_code = 'PP-LS'
        
        self._common_tests()        

        serie = self.db[constants.COL_SERIES].find_one({'provider_name': self.fetcher.provider_name, 
                                                        "dataset_code": self.dataset_code,
                                                        "key": "Q:AU"})
        self.assertIsNotNone(serie)

    def test_eeri(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:BISDatasetsDBTestCase.test_eeri
                
        self.dataset_code = 'EERI'
        
        self._common_tests()        

        serie = self.db[constants.COL_SERIES].find_one({'provider_name': self.fetcher.provider_name, 
                                                        "dataset_code": self.dataset_code,
                                                        "key": "M:N:B:AE"})
        self.assertIsNotNone(serie)


class LightBISDatasetsDBTestCase(BaseDBTestCase):
    """Fetchers Tests - with DB and lights sources
    
    1. Créer un fichier zip à partir des données du dict DATASETS
    
    2. Execute le fetcher normalement et en totalité
    """
    
    # nosetests -s -v dlstats.tests.fetchers.test_bis:LightBISDatasetsDBTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)
        
        self.fetcher = bis.BIS(db=self.db)
        self.dataset_code = None
        self.dataset = None        
        self.filepath = None
        
    @httpretty.activate
    @mock.patch('dlstats.fetchers.bis.BIS_Data.get_store_path', mock_get_store_path)
    @mock.patch('dlstats.fetchers.bis.Downloader.get_filepath', mock_get_filepath)
    def _common_tests(self):

        self._collections_is_empty()

        # Write czv/zip file in local directory
        filepath = get_filepath(self.dataset_code)
        self.assertTrue(os.path.exists(filepath))
        
        url = DATASETS[self.dataset_code]['url']

        httpretty.register_uri(httpretty.GET, 
                               url,
                               body=mock_streaming(filepath),
                               status=200,
                               content_type='application/octet-stream;charset=UTF-8',
                               streaming=True)
        
        self.fetcher.provider.update_database()
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": self.fetcher.provider_name})
        self.assertIsNotNone(provider)
        
        self.fetcher.upsert_data_tree()
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": self.fetcher.provider_name})
        self.assertIsNotNone(provider['data_tree'])

        self.fetcher.upsert_dataset(self.dataset_code)
        
        self.dataset = self.db[constants.COL_DATASETS].find_one({'provider_name': self.fetcher.provider_name, 
                                                            "dataset_code": self.dataset_code})
        self.assertIsNotNone(self.dataset)

        series = self.db[constants.COL_SERIES].find({'provider_name': self.fetcher.provider_name, 
                                                     "dataset_code": self.dataset_code})

        self.assertEqual(series.count(), SERIES_COUNT)

    def test_lbs_diss(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:LightBISDatasetsDBTestCase.test_lbs_diss

        self.dataset_code = 'LBS-DISS'        

        self._common_tests()

        #TODO: meta_datas tests  

    def test_cbs(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:LightBISDatasetsDBTestCase.test_cbs

        self.dataset_code = 'CBS'        

        self._common_tests()

        #TODO: meta_datas tests  
    
    def test_dss(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:LightBISDatasetsDBTestCase.test_dss
        self.dataset_code = 'DSS'        

        self._common_tests()

        #TODO: meta_datas tests  

    def test_cnfs(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:LightBISDatasetsDBTestCase.test_cnfs

        self.dataset_code = 'CNFS'        
        
        self._common_tests()

        #TODO: meta_datas tests  

    def test_dsrp(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:LightBISDatasetsDBTestCase.test_dsrp

        self.dataset_code = 'DSRP'        

        self._common_tests()

        #TODO: meta_datas tests  
    
    def test_pp_ss(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:LightBISDatasetsDBTestCase.test_pp_ss

        self.dataset_code = 'PP-SS'        

        self._common_tests()

        #TODO: meta_datas tests
          
    def test_pp_ls(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:LightBISDatasetsDBTestCase.test_pp_ls

        self.dataset_code = 'PP-LS'        

        self._common_tests()

        #TODO: meta_datas tests  

    def test_eeri(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:LightBISDatasetsDBTestCase.test_eeri

        self.dataset_code = 'EERI'        

        self._common_tests()
        
    def _common_test_agenda(self):

        httpretty.register_uri(httpretty.GET, 
                               "http://www.bis.org/statistics/relcal.htm?m=6|37|68",
                               body=mock_streaming(AGENDA_FP),
                               match_querystring=True,
                               status=200,
                               content_type='application/octet-stream;charset=UTF-8',
                               streaming=True)

    @httpretty.activate
    def test_parse_agenda(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:LightBISDatasetsDBTestCase.test_parse_agenda
        
        self._common_test_agenda()
        
        #first line = column - months

        """Il doit y avoir 26 actions
        
        Not implemented:
        - Derivatives statistics OTC
        - Derivatives statistics Exchange-traded
        - Global liquidity indicators
        - Property prices Detailed data
        - BIS Statistical Bulletin        
        """
        attempt = [
            [None, None, 
             datetime.datetime(2015, 12, 1, 0, 0), 
             datetime.datetime(2016, 1, 1, 0, 0), 
             datetime.datetime(2016, 2, 1, 0, 0), 
             datetime.datetime(2016, 3, 1, 0, 0), 
             datetime.datetime(2016, 4, 1, 0, 0), 
             datetime.datetime(2016, 5, 1, 0, 0)],
            [
                 'Banking statistics',  # dataset CBS
                 'Locational', 
                 '6',   #December 2015 : 6 (Q2 2015+)
                 '22',  #January 2016  : 22* (Q3 2015)
                 None,  #February 2016 : None
                 '6',   #March  2016   : 6 (Q3 2015+)
                 '22',  #April 2016    : 22* (Q4 2015)
                 None   #May 2016      : None
            ],
             ['Banking statistics', 'Consolidated', '6', '22', None, '6', '22', None],
             ['Debt securities statistics', 'International', '6', None, None, '6', None, None],
             ['Debt securities statistics', 'Domestic and total', '6', None, None, '6', None, None],
             ['Derivatives statistics', 'OTC', '6', None, None, '6', None, '13'],
             ['Derivatives statistics', 'Exchange-traded', '6', None, None, '6', None, None],
             ['Global liquidity indicators', None, '6', None, None, '6', None, None],
             ['Credit to non-financial sector', None, '6', None, None, '6', None, None],
             ['Debt service ratio', None, '6', None, None, '6', None, None],
             ['Property prices', 'Detailed data', '18', '22', '19', '18', '22', '20'],
             ['Property prices', 'Selected', None, None, '19', None, None, '20'],
             ['Property prices', 'long', None, None, '19', None, None, '20'],
             ['Effective exchange rates', None, '16', '18', '16', '16', '18', '17'],
             ['BIS Statistical Bulletin', None, '6', None, None, '6', None, None]
        ]
        """
        Rapprochement agenda/actions:
        [
             'Banking statistics',  # dataset CBS
             'Locational', 
             '6',   #December 2015 : 6 (Q2 2015+)
             '22',  #January 2016  : 22* (Q3 2015)
             None,  #February 2016 : None
             '6',   #March  2016   : 6 (Q3 2015+)
             '22',  #April 2016    : 22* (Q4 2015)
             None   #May 2016      : None
        ],
        
        4 Actions:
        
        '6',   #December 2015 : 6 (Q2 2015+)
         {'action': 'update_node', 'kwargs': {'dataset_code': 'CBS', 'provider_name': 'BIS'},
          'period_kwargs': {'run_date': datetime.datetime(2015, 12, 6, 8, 0), 'timezone': ['Europe/Zurich']},
          'period_type': 'date'}
        
        '22',  #January 2016  : 22* (Q3 2015)
         {'action': 'update_node',
          'kwargs': {'dataset_code': 'CBS', 'provider_name': 'BIS'},
          'period_kwargs': {'run_date': datetime.datetime(2016, 1, 22, 8, 0),
                            'timezone': ['Europe/Zurich']},
          'period_type': 'date'},

        '6',   #March  2016   : 6 (Q3 2015+)
         {'action': 'update_node',
          'kwargs': {'dataset_code': 'CBS', 'provider_name': 'BIS'},
          'period_kwargs': {'run_date': datetime.datetime(2016, 3, 6, 8, 0),
                            'timezone': ['Europe/Zurich']},
          'period_type': 'date'},

        '22',  #April 2016    : 22* (Q4 2015)
         {'action': 'update_node',
          'kwargs': {'dataset_code': 'CBS', 'provider_name': 'BIS'},
          'period_kwargs': {'run_date': datetime.datetime(2016, 4, 22, 8, 0),
                            'timezone': ['Europe/Zurich']},
          'period_type': 'date'},
        
        """
        
        agenda = self.fetcher.parse_agenda()
        #print()
        #pprint(agenda, width=120)
        self.assertEqual(agenda, attempt)
                    
    @httpretty.activate
    def test_get_calendar(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_bis:LightBISDatasetsDBTestCase.test_get_calendar

        self._common_test_agenda()
        
        self.maxDiff = None

        calendar = list(self.fetcher.get_calendar())

        self.assertEqual(len(calendar), 26)

        attempt = [
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'LBS-DISS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2015, 12, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'LBS-DISS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 1, 22, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'LBS-DISS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 3, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'LBS-DISS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 4, 22, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'CBS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2015, 12, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'CBS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 1, 22, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'CBS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 3, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'CBS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 4, 22, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'DSS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2015, 12, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'DSS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 3, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'DSS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2015, 12, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'DSS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 3, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'CNFS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2015, 12, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'CNFS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 3, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'DSRP', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2015, 12, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'DSRP', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 3, 6, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'PP-SS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 2, 19, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'PP-SS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 5, 20, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'PP-LS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 2, 19, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'PP-LS', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 5, 20, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'EERI', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2015, 12, 16, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'EERI', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 1, 18, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'EERI', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 2, 16, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'EERI', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 3, 16, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'EERI', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 4, 18, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'},
             {'action': 'update_node',
              'kwargs': {'dataset_code': 'EERI', 'provider_name': 'BIS'},
              'period_kwargs': {'run_date': datetime.datetime(2016, 5, 17, 8, 0),
                                'timezone': ['Europe/Zurich']},
              'period_type': 'date'}]

        self.assertEqual(calendar, attempt)

