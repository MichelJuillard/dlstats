# -*- coding: utf-8 -*-

import tempfile
import datetime
import os
import pandas
from io import StringIO
from pprint import pprint
from urllib.parse import urlparse
from urllib.request import url2pathname, pathname2url

from dlstats.fetchers._commons import Datasets
from dlstats.fetchers import imf
from dlstats import constants

import unittest
from unittest import mock

from dlstats.tests.base import RESOURCES_DIR, BaseTestCase, BaseDBTestCase

# Nombre de série dans les exemples
SERIES_COUNT = 1

PROVIDER_NAME = 'IMF'

DATASETS = {'ifs': {}}

#---Dataset ifs
DATASETS['ifs']["dimensions_count"] = 4 
DATASETS['ifs']["name"] = "ifs"
DATASETS['ifs']["doc_href"] = None
DATASETS['ifs']["last_update"] = datetime.datetime(2015,10,26)
DATASETS['ifs']["filename"] = "IFS_11-05-2015 13-41-38-02"
DATASETS['ifs']["data"] = """"Country Name","Country Code","Indicator Name","Indicator Code","Time Period","Value","Status",
"Country 1","010","Indicator 1","Code1",1980","12.3","",
"Country 2","011","Indicator 2","Code2","1991","12.0","P",
"Country 1","010","Indicator 1","Code1","1979","10.3","",
"Country 2","011","Indicator 2","Code2","2001","22.3","",
"Country 3","012","Indicator 3","Code3","1980Q2","12.9","",
"Country 3","012","Indicator 3","Code3","1980Q3","12.5","",
"""

def get_store_path(self):
    import tempfile
    return os.path.abspath(os.path.join(tempfile.gettempdir(), 
                                        self.dataset.provider_name, 
                                        self.dataset.dataset_code,
                                        "tests"))

def local_get(url, *args, **kwargs):
    "Fetch a stream from local files."
    from requests import Response

    p_url = urlparse(url)
    if p_url.scheme != 'file':
        raise ValueError("Expected file scheme")

    filename = url2pathname(p_url.path)
    response = Response()
    response.status_code = 200
    response.raw = open(filename, 'rb')
    return response

def write_zip_file(zip_filepath, filename, data):
    """Create file in zipfile
    """
    import zipfile

    with zipfile.ZipFile(zip_filepath, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename, data)
        
def get_filepath(dataset_code):
    """Create CSV file in zipfile
    
    Return local filepath of zipfile
    """
    dataset = DATASETS[dataset_code]
    zip_filename = DATASETS[dataset_code]['filename']+'.zip'
    filename = DATASETS[dataset_code]['filename']+'.csv'
    dirpath = os.path.join(tempfile.gettempdir(), PROVIDER_NAME, dataset_code, "tests")
    filepath = os.path.abspath(os.path.join(dirpath, zip_filename))
    
    if os.path.exists(filepath):
        os.remove(filepath)
        
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    
    write_zip_file(filepath, filename, DATASETS[dataset_code]['data'])
    
    return filepath

def load_fake_original_datas(select_dataset_code=None):
    """Load datas from DATASETS dict
    
    key: DATASETS[dataset_code]['datas']
    """
    
    fetcher = imf.IMF()
    
    results = {}
    
    for dataset_code, dataset in DATASETS.items():
        
        if select_dataset_code and select_dataset_code != dataset_code:
            continue
        
        _dataset = Datasets(provider_name=fetcher.provider_name, 
                    dataset_code=dataset_code, 
                    name=dataset['name'], 
                    doc_href=dataset['doc_href'], 
                    fetcher=fetcher, 
                    is_load_previous_version=False)
        
        if dataset_code == 'ifs':
            dataset_datas = imf.IFS_Data(_dataset, is_autoload=False)
            dataset_datas.load_original_data(StringIO(dataset['data']))
        
    return dataset_datas.original_data

def load_fake_datas(select_dataset_code=None):
    """Load datas from DATASETS dict
    
    key: DATASETS[dataset_code]['datas']
    """
    
    fetcher = IMF()
    
    results = {}
    
    for dataset_code, dataset in DATASETS.items():
        
        if select_dataset_code and select_dataset_code != dataset_code:
            continue
        
        _dataset = Datasets(provider_name=fetcher.provider_name, 
                    dataset_code=dataset_code, 
                    name=dataset['name'], 
                    doc_href=dataset['doc_href'], 
                    fetcher=fetcher, 
                    is_load_previous_version=False)
        
        dataset_datas = dataset['data_iterator']
        dataset_datas.load_datas(dataset['datas'])
        
        results[dataset_code] = {'series': []}

        for d in dataset_datas.rows:
            results[dataset_code]['series'].append(dataset_datas.build_serie(d))
            
    #pprint(results)
    return results

class IMFDatasetsTestCase(BaseTestCase):
    """Fetchers Tests - No DB access
    """
    
    # nosetests -s -v dlstats.tests.fetchers.test_imf:IMFDatasetsTestCase
    
    def test_imf_ifs_original_data(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_imf:IMFDatasetsTestCase.test_ifs_original_data        
        datas = load_fake_original_datas('ifs')

        empty_values_a = ["na" for i in range(2015-1947)]
        empty_values_q = ["na" for i in range(4*(2015-1947))]
        empty_values_m = ["na" for i in range(12*(2015-1947))]
        empty_status_a = empty_values_a
        empty_status_q = empty_values_q
        empty_status_m = empty_values_m
        attempt = { "Code1.010.A": {"Country Code": "010", "Indicator Code": "Code1", "frequency": "A",
                                    "values": empty_values_a, "Status": empty_status_a}, 
                    "Code2.011.A": {"Country Code": "011", "Indicator Code": "Code2", "frequency": "A",
                                    "values": empty_values_a, "Status": empty_status_a}, 
                    "Code3.012.Q": {"Country Code": "012", "Indicator Code": "Code3", "frequency": "Q",
                                    "values": empty_values_q, "Status": empty_status_q}}
        attempt["Code1.010.A"]["values"][1979-1947] = '10.3'
        attempt["Code1.010.A"]["values"][1980-1947] = '12.3'
        attempt["Code2.011.A"]["values"][1991-1947] = '12.0'
        attempt["Code2.011.A"]["values"][2001-1947] = '22.3'
        attempt["Code3.012.Q"]["values"][4*(1979-1947)+2] = '12.9'
        attempt["Code3.012.Q"]["values"][4*(1979-1947)+3] = '12.5'
        attempt["Code2.011.A"]["Status"][1991-1947] = "P"

        self.maxDiff = None
        self.assertDictEqual(datas, attempt)

        attemps_country_list = {"010": "Country 1",
                        "011": "Country 2",
                        "012": "Country 3"}
                                         
        self.assertDictEqual(dataset_datas.country_list, attempt_country_list)

        attempt_indicator_list = {"Code1": "Indicator 1",
                                  "Code2": "Indicator 2",
                                  "Code3": "Indicator 3"}
                                         
        self.assertDictEqual(dataset_datas.indicator_list, attempt_indicator_list)

    @unittest.skipIf(True, "TODO")    
    def test_ifs(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_imf:IMFDatasetsTestCase.test_ifs        
        datas = load_fake_datas('ifs')
        print("")
        pprint(datas)

        attempt = {'ifs': {'series': [{'attributes': {},
                                            'datasetCode': 'ifs',
                                            'dimensions': {'FREQ': "A",
                                                           'unit': "CLV05_MEUR",
                                                           'na_item': "B1G",
                                                           'geo': "AT",
                                                           'TIME_FORMAT': "P1Y"},
                                            'endDate': 45,
                                            'frequency': 'A',
                                            'key': 'A.CLV05_MEUR.B1G.AT',
                                            'name': '',
                                  'provider': 'IMF',
                                  'startDate': 25,
                                  'values': ["176840.7", "180307.4", "184320.1"]}]}}        
        self.assertDictEqual(datas, attempt)

        
class IMFDatasetsDBTestCase(BaseDBTestCase):
    """Fetchers Tests - with DB
    
    sources from DATASETS[dataset_code]['datas'] written in zip file
    """
    
    # nosetests -s -v dlstats.tests.fetchers.test_imf:IMFDatasetsDBTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)
        self.fetcher = imf.IMF(db=self.db, es_client=self.es)
        self.dataset_code = None
        self.dataset = None        
        self.filepath = None

    @mock.patch('dlstats.fetchers.bis.BIS_Data.get_store_path', get_store_path)    
    def _common_tests(self):

        self._collections_is_empty()
        
        self.filepath = get_filepath(self.dataset_code)
        self.assertTrue(os.path.exists(self.filepath))
        
        # provider.update_database
        self.fetcher.provider.update_database()
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": self.fetcher.provider_name})
        self.assertIsNotNone(provider)
        
        # upsert_categories
#        self.fetcher.upsert_categories()
#        category = self.db[constants.COL_CATEGORIES].find_one({"provider": self.fetcher.provider_name, 
#                                                               "categoryCode": self.dataset_code})
#        self.assertIsNotNone(category)
        
        dataset = Datasets(provider_name=self.fetcher.provider_name, 
                           dataset_code=self.dataset_code, 
                           name=DATASETS[self.dataset_code]['name'],
                           last_update=DATASETS[self.dataset_code]['last_update'],
                           doc_href=DATASETS[self.dataset_code]['doc_href'], 
                           fetcher=self.fetcher)

        # manual Data for iterator
        fetcher_data = imf.IFS_Data(dataset) 
        dataset.series.data_iterator = fetcher_data
        dataset.update_database()

        self.dataset = self.db[constants.COL_DATASETS].find_one({"provider": self.fetcher.provider_name, 
                                                            "datasetCode": self.dataset_code})
        
        self.assertIsNotNone(self.dataset)
        
        self.assertEqual(len(self.dataset["dimensionList"]), DATASETS[self.dataset_code]["dimensions_count"])
        
        series = self.db[constants.COL_SERIES].find({"provider": self.fetcher.provider_name, 
                                                     "datasetCode": self.dataset_code})
        self.assertEqual(series.count(), SERIES_COUNT)
        
        
    @unittest.skipIf(True, "TODO")    
    def test_ifs(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_imf:IMFDatasetsDBTestCase.test_ifs
                
        self.dataset_code = 'ifs'
        
        self._common_tests()        

        serie = self.db[constants.COL_SERIES].find_one({"provider": self.fetcher.provider_name, 
                                                        "datasetCode": self.dataset_code,
                                                        "key": "A.CLV05_MEUR.B1G.AT"})
        self.assertIsNotNone(serie)
        
        d = serie['dimensions']
        self.assertEqual(d["freq"], 'A')
        self.assertEqual(d["unit"], 'CLV05_MEUR')
        self.assertEqual(d["geo"], 'AT')
        
        #TODO: meta_datas tests  

        #TODO: clean filepath


class LightIMFDatasetsDBTestCase(BaseDBTestCase):
    """Fetchers Tests - with DB and lights sources
    
    1. Créer un fichier zip à partir des données du dict DATASETS
    
    2. Execute le fetcher normalement et en totalité
    """
    
    # nosetests -s -v dlstats.tests.fetchers.test_imf:LightIMFDatasetsDBTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)
        self.fetcher = IMF.IMF(db=self.db, es_client=self.es)
        self.dataset_code = None
        self.dataset = None        
        self.filepath = None
        
    @mock.patch('dlstats.fetchers.bis.BIS_Data.get_store_path', get_store_path)    
    def _common_tests(self):

        self._collections_is_empty()

        # Write czv/zip file in local directory
        filepath = get_filepath(self.dataset_code)
        self.assertTrue(os.path.exists(filepath))
        # Replace dataset url by local filepath
        DATASETS[self.dataset_code]['url'] = "file:%s" % pathname2url(filepath)

        self.fetcher.provider.update_database()
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": self.fetcher.provider_name})
        self.assertIsNotNone(provider)
        
        self.fetcher.upsert_categories()
        category = self.db[constants.COL_CATEGORIES].find_one({"provider": self.fetcher.provider_name, 
                                                               "categoryCode": self.dataset_code})
        self.assertIsNotNone(category)

        self.fetcher.upsert_dataset(self.dataset_code)
        
        self.dataset = self.db[constants.COL_DATASETS].find_one({"provider": self.fetcher.provider_name, 
                                                            "datasetCode": self.dataset_code})
        self.assertIsNotNone(self.dataset)

        series = self.db[constants.COL_SERIES].find({"provider": self.fetcher.provider_name, 
                                                     "datasetCode": self.dataset_code})

        self.assertEqual(series.count(), SERIES_COUNT)

    @unittest.skipIf(True, "TODO")    
    def test_ifs(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_imf:LightIMFDatasetsDBTestCase.test_ifs

        self.dataset_code = 'ifs'        

        self._common_tests()

    @unittest.skipIf(True, "TODO")    
    @mock.patch('dlstats.fetchers.bis.BIS_Data.get_store_path', get_store_path)    
    def test_selected_datasets(self):

        # nosetests -s -v dlstats.tests.fetchers.test_imf:LightIMFDatasetsDBTestCase.test_selected_datasets()

        self.fetcher.upsert_categories()

        self.fetcher.selected_codes = ['ifs']

        datasets = self.fetcher.get_selected_datasets()

        for d in datasets:
            # Write czv/zip file in local directory
            filepath = get_filepath(d)
            self.assertTrue(os.path.exists(filepath))
            # Replace dataset url by local filepath
            DATASETS[d]['url'] = "file:%s" % pathname2url(filepath)

        self.fetcher.upsert_selected_datasets()
        
        
        

        #TODO: meta_datas tests  

@unittest.skipUnless('FULL_REMOTE_TEST' in os.environ, "Skip - not full remote test")
class FullIMFDatasetsDBTestCase(BaseDBTestCase):
    """Fetchers Tests - with DB and real download sources
    
    1. Télécharge ou utilise des fichiers existants
    
    2. Execute le fetcher normalement et en totalité
    """
    
    # FULL_REMOTE_TEST=1 nosetests -s -v dlstats.tests.fetchers.test_imf:FullIMFDatasetsDBTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)
        self.fetcher = IMF.IMF(db=self.db, es_client=self.es)
        self.dataset_code = None
        self.dataset = None        
        self.filepath = None
        
    #@mock.patch('requests.get', local_get)
    def _common_tests(self):

        self._collections_is_empty()

        self.fetcher.provider.update_database()
        provider = self.db[constants.COL_PROVIDERS].find_one({"name": self.fetcher.provider_name})
        self.assertIsNotNone(provider)
        
        self.fetcher.upsert_categories()
        category = self.db[constants.COL_CATEGORIES].find_one({"provider": self.fetcher.provider_name, 
                                                               "categoryCode": self.dataset_code})
        self.assertIsNotNone(category)
        
        self.fetcher.upsert_dataset(self.dataset_code)
        
        self.dataset = self.db[constants.COL_DATASETS].find_one({"provider": self.fetcher.provider_name, 
                                                            "datasetCode": self.dataset_code})
        self.assertIsNotNone(self.dataset)

        series = self.db[constants.COL_SERIES].find({"provider": self.fetcher.provider_name, 
                                                     "datasetCode": self.dataset_code})

        series_count = series.count()
        self.assertTrue(series_count > 1)
        print(self.dataset_code, series_count)

    @unittest.skipIf(True, "TODO")    
    def test_ifs(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_imf:FullIMFDatasetsDBTestCase.test_ifs

        self.dataset_code = 'ifs'        

        self._common_tests()
        
        #self.fail("test")

        #TODO: meta_datas tests  

