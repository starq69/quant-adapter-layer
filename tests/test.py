#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import sys
import conventions

'''
convention (default) over configuration sample
'''
configuration = {}
configuration[conventions._FOO_POLICY_] = 'changed'
configuration[conventions._BAR_POLICY_] = 'changed'

_ACCEPT_CONST_OVERRIDE_ = False
_ACCEPT_CONST_OVERRIDE_ = True

def print_policy(c):

    print('RUNNING print_policy')
    for k, v in c.items():
        print('{} ={}'.format(k, v))


def main():
    ''' 
    for k, v in defaults.items():
        if not k in configuration: configuration[k] = defaults[k]
    '''
    _const = 0

    for k, v in conventions.defaults.items():

        print('conventions.defaults:')
        print('k={} \tv={}'.format(k, v))

        if not k in configuration: 
            '''
            carica il default su configuration
            '''
            print('k={} not in configuration'.format(k))

            configuration[k] = conventions.defaults[k]

        else: # override

            print('k={} in configuration'.format(k))
            print('TEST : k={} in conventions_const:'.format(k))

            if k in conventions.const:

                print('TRUE : warning: try to change a const')
                if not _ACCEPT_CONST_OVERRIDE_:
                    configuration[k] = conventions.defaults[k]

                _const += 1

    if _const and not _ACCEPT_CONST_OVERRIDE_:
            print('one ore more conventions const overrides not accepted')

    print('STARTED')

    print_policy(configuration)

if __name__ == '__main__':
    main()
