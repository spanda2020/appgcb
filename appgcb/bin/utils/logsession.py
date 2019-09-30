"""
This file is utility function called get loggingSession to return a valid logging session
"""

import logging

def getloggingSession():
    # create logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # create console handler and set level to debug
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # create formatter
    formatter = logging.Formatter('%(asctime)s %(levelname)s: '+ 'Line - ' + '%(lineno)d' + ' %(message)s',"%Y%m%d%H%M%S")

    # add formatter to handlers
#    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # add handlers to logger
#    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
