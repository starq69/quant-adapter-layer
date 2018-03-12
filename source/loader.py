#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import logging
from importFromURI import importModule 

def load_adapter(conf, module_name):

    log = logging.getLogger(__name__)

    adapter = importModule(module_name)

    try: 
        adapter.init(conf)

    # except ... raise
    except AttributeError as e:
        log.error('error: {}'.format(e))

    return adapter

