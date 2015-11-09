# -*- coding: utf-8 -*-
"""
Created on Fri Nov  6 16:22:46 2015

@author: salimeh
"""


import csv
import pandas


class IFS_Data():
    def __init__(self):
        self.original_data = {}
        self.country_list= {}
        self.indicatore_list= {}
    def load_original_data(self):
        
        with open('/home/salimeh/IFS/IFS_10-20-2015 20-09-38-08.csv') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader :
                self.frequency = 'A'
                plus_fre = 23    # Default base year in pandas.period is 1970, 1970-1947=23
                if 'Q' in row["Time Period"]:
                    self.frequency = 'Q'
                    plus_fre = 23*4
                if 'M' in row["Time Period"]: 
                    self.frequency = 'M' 
                    plus_fre = 23*12
                self.key = row["Indicator Code"]+'.'+row["Country Code"] +'.'+ self.frequency 
                if self.key in self.original_data.keys():

                    self.original_data[self.key]["Country Code"] = row["Country Code"]
                    self.original_data[self.key]["Indicator Code"] = row["Indicator Code"]
                    self.original_data[self.key]["frequency"] =self.frequency
                    self.original_data[self.key]["values"][pandas.Period(row["Time Period"].replace('M', '-'),self.frequency).ordinal+plus_fre-1] = row["Value"]
                    self.original_data[self.key]["Status"][pandas.Period(row["Time Period"].replace('M', '-'),self.frequency).ordinal+plus_fre-1] = row["Status"]
                
                else:
                    self.original_data[self.key] = {}
                    self.original_data[self.key]["values"] =  ["na" for i in range(2015-1947)]
                    self.original_data[self.key]["Status"] =  ["na" for i in range(2015-1947)]
                    if self.frequency == 'M':                 
                        self.original_data[self.key]["values"] =  ["na" for i in range(12*(2015-1947))]
                        self.original_data[self.key]["Status"] =  ["na" for i in range(12*(2015-1947))]
                    if self.frequency == 'Q':
                        self.original_data[self.key]["values"] =  ["na" for i in range(4*(2015-1947))]
                        self.original_data[self.key]["Status"] =  ["na" for i in range(4*(2015-1947))]
                        
                    self.original_data[self.key]["Country Code"] = row["Country Code"]
                    self.original_data[self.key]["Indicator Code"] = row["Indicator Code"]
                    self.original_data[self.key]["frequency"] =self.frequency
                    self.original_data[self.key]["values"][pandas.Period(row["Time Period"].replace('M', '-'),self.frequency).ordinal+plus_fre-1] = row["Value"]
                    self.original_data[self.key]["Status"][pandas.Period(row["Time Period"].replace('M', '-'),self.frequency).ordinal+plus_fre-1] = row["Status"]
                if row["Country Code"] in self.country_list.keys():
                    pass
                else:
                    self.country_list[row["Country Code"]] =  row['\ufeff"Country Name"']               
                    

                if row["Indicator Code"] in self.indicatore_list.keys():
                    pass
                else:    
                    self.indicatore_list[row["Indicator Code"]] = row["Indicator Name"]
                    

        

        return(self.original_data)
if __name__ == "__main__":
    w = IFS_Data()
    ee= w.load_original_data()
    
        