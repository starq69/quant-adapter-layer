#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import sys, os, logging

this = sys.modules[__name__]

class BaseAdapter():

    def __init__(self, env):
        '''
        attributi base mandatory
        '''
        self.name = 'my base adapter'
        self.DSRoot = env

    def dataSources(self):

        subfolders = [f.name for f in os.scandir(self.DSRoot) if f.is_dir() ]
        return subfolders 

def start(env):

    env = '/home/starq/REP/DATA/FINANCE/Quotazioni/'
    try:
        this.implemented = BaseAdapter(env)
        return True
    except:
        pass
