#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

# Public Domain, i.e. feel free to copy/paste
# Considered a hack in Python 2
import inspect
import logging
import sys


def _L(skip=0):
    '''Shorthand to get logger for current function frame.'''
    return logging.getLogger(caller_name(skip + 1))


def caller_name(skip=2):
    """Get a name of a caller in the format module.class.method

       `skip` specifies how many levels of stack to skip while getting caller
       name. skip=1 means "who calls me", skip=2 "who calls my caller" etc.

       An empty string is returned if skipped levels exceed stack height
    """
    def stack_(frame):
        framelist = []
        while frame:
            framelist.append(frame)
            frame = frame.f_back
        return framelist

    stack = stack_(sys._getframe(1))
    start = 0 + skip
    if len(stack) < start + 1:
        return ''
    parentframe = stack[start]

    name = []
    module = inspect.getmodule(parentframe)
    # `modname` can be None when frame is executed directly in console
    # TODO(techtonik): consider using __main__
    if module:
        name.append(module.__name__)
    # detect classname
    if 'self' in parentframe.f_locals:
        # I don't know any way to detect call from the object method
        # XXX: there seems to be no way to detect static method call - it will
        #      be just a function call
        name.append(parentframe.f_locals['self'].__class__.__name__)
    codename = parentframe.f_code.co_name
    if codename != '<module>':  # top level usually
        name.append(codename)  # function or a method
    del parentframe
    return ".".join(name)

# https://stackoverflow.com/questions/17065086/how-to-get-the-caller-class-name-inside-a-function-of-another-class-in-python
def get_class_from_frame(fr):
  args, _, _, value_dict = inspect.getargvalues(fr)
  # we check the first parameter for the frame function is
  # named 'self'
  if len(args) and args[0] == 'self':
    # in that case, 'self' will be referenced in value_dict
    instance = value_dict.get('self', None)
    if instance:
      # return its class
      return getattr(instance, '__class__', None)
  # return None otherwise
  return None

# https://stackoverflow.com/questions/29530443/how-to-get-the-caller-of-a-method-in-a-decorator-in-python
#
def protected(f):      
    '''
    deve analizzare il caller della funzione decorata :
    se caller.__class__ == Connection 
    oppure
    caller.__module__ == lo stessom modulo : OK (il metodo è protetto ovvero può essere invocato solo dalla classe o dal modulo in cui è definito
    '''
    def inner_func(calling_obj, *args, **kwargs):
        if args and hasattr(args[0], '__class__'):
            print('__class__: {}'.format(args[0].__class__)) #  restituisce la __class__ del primo parametro della func decorata .. nb deve fare tutt'altro (vedi sopra)
        else:
            print('no arg or not a __class__ member')
        
        return f(calling_obj, *args, **kwargs) 
    return inner_func
