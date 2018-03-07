#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-
'''
quant2018 eoddata adapter
'''
import os, sys, logging

this = sys.modules[__name__]


def init(config):

    '''logging.basicConfig(level=logging.DEBUG, format='%(message)s')
    '''
    log = logging.getLogger(__name__)

    '''this.DSRoot = config.get_attr(__name__, 'DSRoot')
    '''
    this.name = 'my base module adapter'
    this.DSRoot = config


def dataSources():
    '''
    es.
    DSRoot subfolders list
    '''
    subfolders = [f.name for f in os.scandir(this.DSRoot) if f.is_dir() ] 

    return subfolders

