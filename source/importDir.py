#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-


#--------------------------------------------
# http://gitlab.com/aurelien-lourot/importdir
#--------------------------------------------

import os
import re
import sys

#-------------------------------
# Interface
#-------------------------------

###@starq69: ex do()
def load_dir_modules(path, env, exclude=None): ###@starq69: exclude=module to be excluded
    """ Imports all modules residing directly in directory "path" into the provided environment
        (usually the callers environment). A typical call:
        importdir.do("example_dir", globals())
    """
    __do(path, env, exclude)


#------------------------------
# Implementation
#------------------------------

# File name of a module:
__module_file_regexp = "(.+)\.py(c?)$"

def __get_module_names_in_dir(path, exclude=None):
    """ Returns a set of all module names residing directly in directory "path".
    """
    result = set()
    if exclude: exclude=set(exclude)

    # Looks for all python files in the directory (not recursively) and add their name to result:
    for entry in os.listdir(path):
        if os.path.isfile(os.path.join(path, entry)):
            regexp_result = re.search(__module_file_regexp, entry)
            if regexp_result: # is a module file name
                if not (regexp_result.groups()[0] in exclude):
                    print(regexp_result.groups()[0])
                    result.add(regexp_result.groups()[0])

    return result

def __do(path, env, exclude=None):
    """ Implements do().
    """
    sys.path.append(path) # adds provided directory to list we can import from
    for module_name in sorted(__get_module_names_in_dir(path, exclude)): # for each found module...
        env[module_name] = __import__(module_name)              # ... import

