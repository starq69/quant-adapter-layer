#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import os, sys
from importFromURI import importModule as modimport
#import click

def load_adapters(conf, module_name):
    '''
    base_dir   = os.path.dirname(os.path.realpath(__file__))
    parent_dir = os.path.split(base_dir)[0]
    root_dir = base_dir
    '''
    adapter = modimport(module_name)
    
    try: 
        # stuff da rimuovere
        test = adapter.test_func
        print('imported adapter={}'.format(str(test())))

        adapter.initialize(conf)

    except AttributeError as e:
        print('error!') 

    return adapter

if __name__ == '__main__':

    conf = 'TBD'

    try:
        stub_adapter = load_adapters(conf, 'stub_adapter')

    except AttributeError as e:
        print ('error: {}'.format(str(e)))

    

