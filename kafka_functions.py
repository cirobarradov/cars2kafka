import argparse
import json
from pprint import pprint
import pkg_resources
import sys
import json
from confluent_kafka import  KafkaError, KafkaException
from abc import ABC, abstractmethod
from os import listdir
from os.path import isfile, join
from pprint import pprint

SOURCE_VALUE="source_value"
SOURCE_TYPE="source_type"
KEY="key"
FORMAT_VALUE="format_value"
JSON="json"
#TODO Logging
class KafkaInput(ABC):
    def __init__(self, configuration_json_file=None):
        '''
        CONSTANTS
        '''
        self._DEBUG_MODE="debug_mode"
        self._TOPIC="topic"
        self._KEY=KEY
        self._SOURCE_TYPE=SOURCE_TYPE
        self._SOURCE_VALUE=SOURCE_VALUE
        self._FORMAT_VALUE=FORMAT_VALUE
        #source type values
        self._SOURCE_TYPE_MESSAGE = "message"
        self._SOURCE_TYPE_FILE = "file"
        self._SOURCE_TYPE_FOLDER = "folder"
        #self.user_json='user.json'
        self._SCHEMA_KEY_FILE = "schema_key_file"
        self._SCHEMA_VALUE_FILE = "schema_value_file"
        #format types
        self._JSON=JSON
        
        #self.user_json='user.json'
        self.producer_json='producer_properties.json'
        self._properties = dict()
        self._mandatory_inputs = list()
        config_json_path = pkg_resources.resource_filename(sys.modules[self.__module__].__name__,configuration_json_file )
        # Read json config
        with open(config_json_path) as f:
            self._config = json.load(f)

        # Load properties
        for k, v in self._config.items():
            self.make_variable(k, v)

        self.make_variable('default')

        for param in self._mandatory_inputs:
            if param not in self.__dict__.keys():
                raise ValueError('ERROR: param "{0}" is MANDATORY'.format(param))
        #pprint(self.__dict__)

    @property
    def properties(self):
        return self._properties

    def make_variable(self, k, v=True):

        inner_config_file = pkg_resources.resource_filename(sys.modules[self.__module__].__name__, self.producer_json)


        # Read json config
        with open(inner_config_file) as f:
            inner_config = json.load(f)

        self.__dict__[k] = v

        if k in inner_config.keys():

            if isinstance(v, bool):
                for k2, v2 in inner_config[k].items():
                    self.add_property(k2, v2)

            elif isinstance(v, str):
                current_selection = inner_config[k][v]
                for k2, v2 in current_selection.items():
                    self.add_property(k2, v2)

            else:
                self.add_property(k,inner_config[k])

    def add_property(self, name, value):
        if name == 'mandatory_inputs':
            self._mandatory_inputs += value
        else:
            self.properties[name] = value

"""
kafka utils
"""
class KafkaUtils():
    def parse_args(self):
        parser = argparse.ArgumentParser(description='Get Kafka Producer params ')
        parser.add_argument("-v", "--{}".format(SOURCE_VALUE), type=str, help='message value to send: [message, file path or folder path]', required=False)    
        args = parser.parse_args()
        return vars(args)    
    
    def prepareFormat(self, data, format):
        res = data
        if format == JSON:
            res= json.loads(data)
        return res

    def getKeyValue(self,key,value,config):
        kwargs=self.parse_args()
        source_type=config.get(SOURCE_TYPE)
        if (source_type == "message"):
            value = (value, kwargs.get(SOURCE_VALUE))[value is None]
            if (value is None):
                raise ValueError('ERROR: source_value must be introduced')
        else:
            ValueError('source type to manage{}'.format(source_type))         
        
        value=self.prepareFormat(value,config.get(FORMAT_VALUE))
        key = (key,config.get(KEY))[key is None]
        return (key,value)

    def error_cb(self, err):
        print ('error_cb --------> {}'.format(err))
        if err.code() == KafkaError._ALL_BROKERS_DOWN:            
            raise KafkaException('ERROR: all brokers down...')            
        else:      
            print(err.code())            
            raise KafkaException(err.code())
        
    def delivery_callback(self,err, msg):
        '''
        Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). 
        
        '''
        if err is not None:
            print('Message {} delivery failed: {}'.format(msg.value(), err))
        else:
            print('Message {} delivered to {} [{}]'.format(msg.value(), msg.topic(), msg.partition()))
    
    def getFiles(self,value):
        files = [f for f in listdir(value) if isfile(join(value, f))]      
        return files

if __name__ == '__main__':

    p = KafkaInput('user.json')

