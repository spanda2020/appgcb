import configparser

config = configparser.RawConfigParser()
#changing PATH of pipeline.ini to config folder
config.read('config/dev_param_detl.ini')
