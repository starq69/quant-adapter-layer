#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import sys, os, logging, configparser
from loader import load_adapter
import ds_settings as ds_global_settings
from merge_settings import merge_settings

__all__ = ['Connection']

_models = {}


class Connection(object):

    def __init__(self, app_config, model, datasource):

        self.log        = logging.getLogger(__name__)
        func_name       = sys._getframe().f_code.co_name
        self.log.info('==> Running {}()'.format(func_name))

        self._CONFIG_SECTION_ = 'DATASOURCE' ### nb utilizzato dopo la config.read : può essere un val configurato

        base_dir        = os.path.dirname (os.path.realpath(__file__))
        parent_dir      = os.path.split (base_dir)[0] ### essendo cfg_file nella parent dir...
        self.app_config = app_config
        self.model      = model.strip()
        self.datasource = datasource.strip()
        self.cfg_file   = parent_dir + '/' + self.datasource + '.ini'

        if model not in _models: 
            try:
                _models[model] = load_adapter('TBD', model) 
                self.log.info('==> model <{}> loaded'.format(model))

            except Exception as e:
                self.log.error('fail to load model {} : ABORT'.format(model, e))
                sys.exit(1)
        else:
                self.log.info('==> model <{}> already loaded'.format(model))


        try:                                                                                              
            configured = configparser.ConfigParser () 

            if not configured.read (self.cfg_file):          ### Return list of successfully read files
                self.log.warning ('missing datasource configuration file <{}> ... try to load defaults...'.format(self.cfg_file))

            self.ds_run_settings = merge_settings (ds_global_settings, configured, self._CONFIG_SECTION_)

            self.log.debug('--------self.ds_run_settings.items()-----------------')
            for k, v in self.ds_run_settings.items(): self.log.debug('[{}] = {}'.format(k, v))
            self.log.debug('-----------------------------------------------------')

        except configparser.Error as e:
            self.log.error ('configparser.Error in Connection() : {} --> ABORT'.format (e))
            sys.exit(1)

        self.log.info('<== leave {}()'.format(func_name))



    def __enter__(self):

        func_name = sys._getframe().f_code.co_name
        self.log.info('==> Running {}()'.format(func_name))

        model   = _models[self.model]
        ds      = self.datasource

        model.init (self.ds_run_settings, ds_global_settings) 

        #model.registerConnection(ds) ###TBD: new name: load_datasource
        model.load_schema(ds)  
        '''
        model.load_index (ds)
        model.load_cache (ds)
        '''
        self.log.info('<== leave {}()'.format(func_name))

        return self


    def __exit__(self, e_typ, e_val, trcbak):
        self.log.info('TBD : model.release_datasource(ds)')
        pass


    def select(self, query):

        _model = _models [self.model]
        func_name = sys._getframe().f_code.co_name
        self.log.info('==> Running {}() on model <{}>'.format(func_name, _model.__name__))

        self.log.info('ok')

        self.log.info('<== leave {}()'.format(func_name))


    ''' NEW '''
    def ingest (self, resource=None):

        func_name = sys._getframe().f_code.co_name
        self.log.info('==> Running {}(resource={})'.format(func_name, resource if resource else '<None>'))

        model   = _models[self.model]

        _K_, _V_ = ds_global_settings, self.ds_run_settings

        if resource is not None:
            try:
                if os.path.isdir(resource):
                    '''
                    ingest directory
                    '''
                    self.log.debug('ingest DIRECTORY')
                    _files = model.get_file_items (resource, _V_ [ _K_._INGEST_DEFAULT_FILE_PATTERN_ ])

                    if not _files:
                        self.log.warning('NO FILES TO INGEST FOUND')
                        return None

                    log.debug('files to ingest : {}'.format(_files))

                elif os.path.is_file(resource):
                    '''
                    ingest file
                    '''
                    self.log.debug('ingest FILE')
                    ###TBD : check if valid file to ingest
                    self.log.debug('file to ingest : {}'.format(resource))

            except OSError as e:
                pass
        else:
            try:
                if _V_ [ _K_._SCHEMA_SCAN_OPTION_ ] != _K_._SCHEMA_DS_ROOT_ONLY_:
                    self.log.debug ('Ingest from default location if scan option is not schema_ds_root_only is NOT YET IMPLEMENTED')
                    sys.exit(1)

                self.log.debug('ingest from DEFAULT LOCATION')

                path = _V_ [ _K_._INGEST_DEFAULT_PATH_ ]
                _files = model.get_file_items (path, _V_ [ _K_._INGEST_DEFAULT_FILE_PATTERN_ ])

                if not _files:
                    self.log.warning('NO FILES TO INGEST FOUND')
                    return None

                self.log.debug('_files to ingest : {}'.format(_files))
                '''
                for i,/ (k, v) in enumerate(self.schema.items()):
                    print("index: {}, key: {}, value: {}".format(i, k, v))
                '''
            except FileNotFoundError as e:
                self.log.error('FileNotFoundError : {}'.format(e))


        model._ingest (self.datasource, _files)

        self.log.info('<== leave {}()'.format(func_name))
