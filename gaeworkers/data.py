'''
Module taking care about saving and restoring worker's data
to and from memcache.

Created on 2011-09-02

@author: Xion
'''
from google.appengine.ext import db


class DataError(Exception):
    ''' Exception signaling invalid/corrupt data. '''
    pass


def save_value(value):
    '''
    Saves the value into memcacheable dictionary which can be converted back
    by calling restore_value.
    '''
    if value is None:   return None
    
    # get the value's type name
    class_ = getattr(value, '__class__', None)
    if class_:
        type_ = class_
        module = getattr(class_, '__module__', None)
        if module and module != '__builtin__':
            type_ = module + "." + class_
    else:
        type_ = type(value).__name__
        
    # handle special cases (lots of them, probably)
    if isinstance(value, db.Model):
        entity_protobuf = db.model_to_protobuf(value)
        value = str(entity_protobuf)
    elif isinstance(value, db.Key):
        value = str(value)
    elif isinstance(value, db.Query):
        # TODO: implement this using cursors 'n stuff. Yes, it will likely be quite hard.
        value = repr(value)
    else:
        value = repr(value)
        
    return dict(type = type_, value = value)
        

def restore_value(saved_value):
    '''
    Restores the value from memcached dictionary.
    '''
    try:
        value_type = saved_value['type']
        value_repr = saved_value['value']
    except KeyError:
        raise DataError, "Could not find type & value in %s" % repr(saved_value)
    
    # obtain the class from value's representation
    if '.' in value_type:
        try:
            module, class_name = value_type.rsplit('.', 1)
            class_ = __import__(module, globals(), locals(), fromlist = [class_name])
        except ImportError:
            raise DataError, "Could not import class %s" % value_type
    else:
        try:                class_ = globals()[value_type]
        except KeyError:    raise DataError, "Type %s not found" % value_type

    # handle special cases
    # TODO: make it more intelligent, like a mapping of (de)serializer classes
    if issubclass(class_, db.Model):
        value = db.model_to_protobuf(value_repr)
    elif class_ in [list, dict] or issubclass(class_, basestring):
        value = eval(value_repr)
    else:
        value = class_(value_repr)

    return value
