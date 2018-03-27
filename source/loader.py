#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import logging
from importFromURI import importModule 

###@starq69: MVC
adapters = {}

def load_adapter(conf, module_name):
    '''
    N.B.
    poichè l'adapter ignora l'applicazione è necessario il parametro
    conf che è la configurazione applicativa ?
    '''
    log = logging.getLogger(__name__)

#    if module_name not in adapters:

    adapter = importModule(module_name)
    adapters[module_name] = adapter

#    else:
#        adapter = adapters[module_name]

    try: 
        '''
        adapter.init() quindi carica la configurazione dell'adapter (adapter-name.conf)
        non necessaria a livello applicativo
        '''
        #adapter.init(conf) ### rimuovere conf

    # except ... raise
    except AttributeError as e:
        log.error('error: {}'.format(e))

    return adapter

