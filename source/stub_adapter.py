#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-
'''
quant2018 eoddata adapter
'''
import sys

this = sys.modules[__name__]
this.attr = {'name': None, 
             'description' : None
            } 

#stuff
def test_func():
    return __name__

def initialize(config):
    print('initialized')

    #this['description'] = config.get_attr(__name__, 'description')



def load_data():
    pass

'''
legge il file di configurazione:
cerca l'elemento __name__ nella sezione [Data Adapters]
ok:
legge gli attributi

'''
