#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import os, sys
#from importFromURI import importModule as modimport
#import click
from importDir import load_dir_modules

def load_adapters():

    base_dir   = os.path.dirname(os.path.realpath(__file__))
    #parent_dir = os.path.split(base_dir)[0]

    root_dir = base_dir

    ###@starq69
    # moduli da escludere sulla cartella corrente ('adapters')
    #
    exclude=['__init__', 'startup_system', 'importDir']

    load_dir_modules(".", globals(), exclude)

    ###@starq69: ..ho importato importFromURI.py : 
    adapter = importFromURI
    test = adapter.test_func
    print('test={}'.format(str(test())))

if __name__ == '__main__':

    load_adapters()
