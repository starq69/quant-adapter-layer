#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-
'''
https://stackoverflow.com/questions/301134/dynamic-module-import-in-python
https://stamat.wordpress.com/2013/06/30/dynamic-module-import-in-python/
'''
import os
import imp
import logging

'''
ex importFromURI(...)
'''
def importModule(uri, absl=False):   

    log = logging.getLogger(__name__)

    if not absl:
        uri = os.path.normpath(os.path.join(os.path.dirname(__file__), uri))

    path, fname = os.path.split(uri)
    mname, ext = os.path.splitext(fname)
            
    no_ext = os.path.join(path, mname)
            
    if os.path.exists(no_ext + '.pyc'):
        try:
            log.debug('compiled')
            return (imp.load_compiled(mname, no_ext + '.pyc'))
        except:
            pass

    if os.path.exists(no_ext + '.py'):
        try:
            log.debug('source')
            return (imp.load_source(mname, no_ext + '.py'))
        except:
            pass

    log.warning('missing module')
