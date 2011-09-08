'''
Module taking care about saving and restoring worker's data
to and from memcache.

Created on 2011-09-02

@author: Xion
'''
from google.appengine.ext import db
import logging


class DataError(Exception):
    ''' Exception signaling invalid/corrupt data. '''
    pass 


def save_value(value):
    '''
    Saves the value into memcacheable pair which can be converted back
    by calling restore_value.
    '''
    if value is None:   return None
    
    for dh_class in _list_data_handlers():
        dh = dh_class()
        if dh.can_save(value):
            return dh.save(value)
    
    logging.warning("[gae-workers] Failed to save value '%s' using specialized data handler; using default", value)
    return (_get_type_name(value), repr(value))
        

def restore_value(saved_value):
    '''
    Restores the value from memcached pair.
    '''
    try:
        type_name, value_repr = saved_value
    except KeyError:
        logging.error("[gae-workers] Invalid format of saved value '%s'", saved_value)
        return None
    
    for dh_class in _list_data_handlers():
        dh = dh_class()
        if dh.can_restore(type_name, value_repr):
            return dh.restore(type_name, value_repr)
        
    logging.warning("[gae-workers] Failed to restore value '%s' using specialized data handler; using default", saved_value)
    return _find_type(type_name)(value_repr)


###############################################################################
# Data handlers for different object types

class DataHandler(object):
    '''
    Base class for data handlers.
    '''
    priority = 0    # the lower the priority, the sooner handler's can_save/can_restore is invoked
    
    def can_save(self, value):
        '''
        Checks whether saving this value is supported by this data handler.
        If it does, then save() will be used to obtain memcache-friendly
        representation of the value.
        '''
        raise NotImplementedError()
        
    def save(self, value):
        '''
        Saves the value and returns pair (type_name, value_representation)
        which can be then converted back by restore(). 
        @return: Saved value, as pair. It should be memcache-friendly, i.e. picklable.
        '''
        raise NotImplementedError()
    
    def can_restore(self, type_name, value_repr):
        '''
        Checks whether restoring saved value of this type is supported by this data handler.
        If it is, then restore() will be used to recover the original value.
        '''
        raise NotImplementedError()
    
    def restore(self, type_name, value_repr):
        ''' Restores original value from result returned by save(). '''
        raise NotImplementedError()
    
    

class DbModelHandler(DataHandler):
    '''
    Data handler for db.Model instances.
    '''
    def can_save(self, value):
        return isinstance(value, db.Model)
    def can_restore(self, type_name, value_repr):
        return _find_type(type_name) == db.Model
    
    def save(self, value):
        entity_protobuf = db.model_to_protobuf(value)
        return (_get_type_name(value), str(entity_protobuf))
    def restore(self, type_name, value_repr):
        return db.model_from_protobuf(value_repr)
    
    
class CollectionsHandler(DataHandler):
    '''
    Data handler for built-in collections.
    '''
    def can_save(self, value):
        return isinstance(value, (list, dict))
    def can_restore(self, type_name, value_repr):
        return type_name in ['list', 'dict']
    
    def save(self, value):
        return (_get_type_name(value), repr(value))
    def restore(self, type_name, value_repr):
        return eval(value_repr)
    
    
class StringHandler(DataHandler):
    '''
    Data handler for strings.
    '''
    def can_save(self, value):
        return isinstance(value, basestring)
    def can_restore(self, type_name, value_repr):
        return type_name in ['str', 'unicode']
    
    def save(self, value):
        return (_get_type_name(value), unicode(value))
    def restore(self, type_name, value_repr):
        return _find_type(type_name)(value_repr)
        

###############################################################################
# Utility functions

def _list_data_handlers():
    '''
    Lists all data handler classes, sorted by descending priority.
    '''
    data_handlers = DataHandler.__subclasses__()
    data_handlers = sorted(data_handlers, key = lambda dh: dh.priority)
    return data_handlers
    

def _get_type_name(value):
    '''
    Retrieves type name from given value.
    @return: Type name, probably in the form module.class_name
    '''
    class_ = getattr(value, '__class__', None)
    if class_:
        type_ = class_
        module = getattr(class_, '__module__', None)
        if module and module != '__builtin__':
            type_ = module + "." + class_
    else:
        type_ = type(value).__name__
        
    return type_

def _find_type(type_name):
    '''
    Utility function that finds type based on given name.
    @param type_name: Class name in the form: module.class_name or just class_name
    @return: Class object or None 
    '''
    if '.' in type_name:
        try:
            module, class_name = type_name.rsplit('.', 1)
            class_ = __import__(module, globals(), locals(), fromlist = [class_name])
        except ImportError:
            raise DataError, "Could not import class %s" % type_name
    else:
        try:                class_ = globals()[type_name]
        except KeyError:    raise DataError, "Type %s not found" % type_name
        
    return class_
