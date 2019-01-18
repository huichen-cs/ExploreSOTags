import errno
import logging
import os
import sys


def setup_logging(module_list = [], 
                  logger_list = [],
                  level = logging.WARNING, 
                  module_logger_to_return = None, 
                  log_path = None, 
                  log_file_base_name = None):
    fmt = '%(asctime)s,%(msecs)d %(process)d %(name)s %(levelname)s %(message)s'
    datefmt = '%H:%M:%S'
    logging.basicConfig(stream=sys.stderr,
                        filemode='a',
                        format=fmt,
                        datefmt=datefmt,
                        level=logging.WARN)
    
    for m in module_list:
        if type(m) is tuple:
            logging.getLogger(m[0].__name__).setLevel(m[1])
        else:
            logging.getLogger(m.__name__).setLevel(level)
    for l in logger_list:
        if type(l) is tuple:
            l.setLevel(l[1])
        else:
            l.setLevel(level)
     
    if log_path != None: 
        try:
            os.makedirs(log_path)
        except OSError as exc: 
            if exc.errno == errno.EEXIST and os.path.isdir(log_path):
                pass
            else:
                raise
             
        log_file_base_path = log_path + '/' + log_file_base_name   
        __add_logging_file_handler(log_file_base_path, module_list, logger_list)
        
    
    if module_logger_to_return != None:
        logger = logging.getLogger(module_logger_to_return.__name__)
        return logger
    else:
        return None
        
def get_module_by_name(name):
    return sys.modules[__name__]        

def __add_logging_file_handler(log_file_base_path, module_list, logger_list=None):
    logFilePath = log_file_base_path + '.log'
    file_handler = logging.handlers.TimedRotatingFileHandler(filename = logFilePath, 
                                                             when = 'D', 
                                                             interval = 7,
                                                             backupCount = 30)
    fmt = '%(asctime)s,%(msecs)d %(process)d %(name)s %(levelname)s %(message)s'
    datefmt = '%H:%M:%S'    
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    file_handler.setFormatter(formatter)

    for m in module_list:
        if type(m) is tuple:
            logger = logging.getLogger(m[0].__name__)
        else:
            logger = logging.getLogger(m.__name__)
        logger.addHandler(file_handler)
        
    for l in logger_list:
        if type(l) is tuple:
            logger = l[0]
        else:
            logger = l
        logger.addHandler(file_handler)