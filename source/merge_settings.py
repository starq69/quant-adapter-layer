#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import sys, logging

def merge (global_settings, configuration, section=None): ###TBD: inglobare nella init()

    func_name = sys._getframe().f_code.co_name
    log = logging.getLogger(__name__)
    log.info('==> Running {}({}, {})'.format(func_name, global_settings, configuration))

    '''
    for each_section in configuration.sections():
        print(each_section)
        for (each_key, each_val) in configuration.items(each_section):
            print(each_key)
            print(each_val)
    '''

    def set_val (default, configured):
        log.debug('enter set_val()')
        try:
            if type(default) is list:
                # se non vuoto (configured=)
                if configured: 
                    return list (set (list (default)  + configured.split(',')))  
                else:
                    return list (set (list (default)))  
            else:
                return configured 
        except Exception as e:
            log.error('Internal error in set_val() with item of type list! :{}'.format(e))
            raise


    if not section: section = 'MODEL' 
    else: section=section.strip() 

    if section in configuration.sections():

        log.debug('section [{}] founded in configuration'.format(section))
        configuration = dict(configuration.items(section))
        if not configuration:
            log.warning('configuration section [{}] is empty : try to load defaults'.format(section))

    else:
        configuration = {}
        log.warning('No configuration section [{}] founded : try to load defaults'.format(section))


    run_settings = {}
    _K_, _V_ = global_settings, run_settings ### 

    try:
        for k, v in global_settings.defaults.items():

            #_msg = 'global_settings.default.item : [{}] = {}'.format(k, v)

            if k in configuration:

                #log.debug(_msg + ' + found in config')

                if not (k in global_settings.const):

                    #log.debug('variable')
                    run_settings[k] = set_val (v, configuration[k]) # ex run_settings[k] = configuration[k]

                elif _K_._ACCEPT_CONST_OVERRIDE_ in configuration and (configuration[_K_._ACCEPT_CONST_OVERRIDE_]): #

                    #log.debug('const : config accept_const_override = True ==> variable')
                    run_settings[k] = set_val (v, configuration[k]) # ex run_settings[k] = configuration[k]

                else:

                    if global_settings.defaults [ _K_._ACCEPT_CONST_OVERRIDE_ ] : 
                                
                        #log.debug('const : config accept_const_override = True ==> variable')
                        run_settings[k] = set_val (v, configuration[k]) # ex run_settings[k] = configuration[k]
                    else:
                        #log.debug('accept_const_override NOT CONFIGURED')
                        run_settings[k] = global_settings.defaults[k]

            else:
                #log.debug(_msg + ' - NOT found in config')
                run_settings[k] = global_settings.defaults[k]

    except Exception as e:
                log.error('merge_policy exception : {}'.format(e))

#    log.debug('merged configuration :')
#    for k, v in _V_.items(): log.debug('[{}] = {}'.format(k, v)) 

    log.info('<== leave {}()'.format(func_name))

    return _V_ ### return dict
