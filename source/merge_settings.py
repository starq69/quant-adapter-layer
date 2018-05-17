#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import sys, logging

def merge_settings (global_settings, configured, section='GLOBALS', debug=False): 
    '''
    global_settings     :   a module with some variables + defaults dict
    configured          :   a configParser object 
    section             :   .ini [GLOBALS] section as default
    debug               :   True => some additional debug log 
    '''

    func_name = sys._getframe().f_code.co_name
    log = logging.getLogger(__name__)
    log.info('==> Running {}({}, {})'.format(func_name, global_settings, configured))

    if debug:
        for each_section in configured.sections():
            print(each_section)
            for (each_key, each_val) in configured.items(each_section):
                print(each_key)
                print(each_val)

    def set_val (default, configured):
        #log.debug('enter set_val()')
        try:
            if type (default) is list:
                # se non vuoto (configured=)
                if configured: 
                    #TBD:  valutare se gestire la policy _LIST_ITEM_MERGE_MODE_ (_LIST_ITEM_MERGE_APPEND_, _LIST_ITEM_MERGE_OVERRIDE)
                    return list (set (list (default)  + configured.split(',')))  
                else:
                    return list (set (list (default)))  
            else: ### scalare
                return configured 
        except Exception as e:
            log.error('Internal error in set_val() with item of type list! :{}'.format(e))
            raise ###

    section=section.strip()

    if section in configured.sections():

        log.debug('section [{}] founded in configuration'.format(section))

        configured = dict(configured.items(section))
        if not configured:
            log.warning('configuration section [{}] is empty : try to load defaults'.format(section))
    else:
        configured = {}
        log.warning('No configuration section [{}] founded : try to load defaults'.format(section))


    run_settings    = {}
    _K_             = global_settings

    try:
        for k, v in _K_.defaults.items():

            _msg = 'global_settings.default.item : [{}] = {}'.format(k, v)

            if k in configured:
                log.debug(_msg + ' + found in config')

                if not (k in global_settings.const):
                    #log.debug('variable')
                    run_settings[k] = set_val (v, configured[k]) 

                elif _K_._ACCEPT_CONST_OVERRIDE_ in configured and (configured[_K_._ACCEPT_CONST_OVERRIDE_]): #
                    #log.debug('const : config accept_const_override = True ==> variable')
                    run_settings[k] = set_val (v, configured[k]) 

                else:

                    if global_settings.defaults [ _K_._ACCEPT_CONST_OVERRIDE_ ] : 
                        #log.debug('const : config accept_const_override = True ==> variable')
                        run_settings[k] = set_val (v, configured[k]) 
                    else:
                        #log.debug('accept_const_override NOT CONFIGURED')
                        run_settings[k] = global_settings.defaults[k]

            else:
                log.debug(_msg + ' - NOT found in config')
                run_settings[k] = global_settings.defaults[k]

    except Exception as e:
                log.error('merge_policy exception : {}'.format(e))

    if debug:
        log.debug('merged configuration :')
        for k, v in _V_.items(): log.debug('[{}] = {}'.format(k, v)) 

    log.info('<== leave {}()'.format(func_name))

    #return _V_ ### return dict
    return run_settings
