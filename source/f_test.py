#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import sys

def frame():
    a = sys._getframe(0).f_globals.get('__name__') ### f_test
    b = sys._getframe(1).f_globals.get('__name__') ### __main__

    for k, v in sys._getframe(1).f_globals.items():
        print('key= {}, \t\t\tval={}'.format(k, v))

    return a

