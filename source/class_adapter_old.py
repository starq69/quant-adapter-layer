#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import sys, os, logging

this = sys.modules[__name__]

class BaseAdapter():

    def __init__(self, env):

        self.name = 'my base class adapter'
        self.DSRoot = env

    def dataSources(self):

        subfolders = [f.name for f in os.scandir(self.DSRoot) if f.is_dir() ]
        return subfolders 

def start(env):

    try:
        this.implemented = BaseAdapter(env)
        return True
    except:
        # log
        return False 
