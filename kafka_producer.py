
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka import Producer
from confluent_kafka import KafkaException

import sys
from kafka_functions import KafkaInput,KafkaUtils

class KafkaProducer(KafkaInput):

    def __init__(self, config_json_path):
        """
        - get producer instance depending on its schema configuration (simple producer or avroproducer)
        - prepare paramters to send values to broker
        """        
        KafkaInput.__init__(self,config_json_path)
        self.utils=KafkaUtils()
        self.producer=self.getProducer()    

    def getProducer(self):
        # return a producer instance
        # :param: producer configuration
        self._properties['error_cb']=self.utils.error_cb
        
        if (self._config.get(self._SCHEMA_KEY_FILE)== None and self._config.get(self._SCHEMA_VALUE_FILE)== None):
            producer = Producer(self._properties)
        elif (self._config.get(self._SCHEMA_KEY_FILE)== None):
            value_schema = avro.load(self._config.get(self._SCHEMA_VALUE_FILE))
            producer = AvroProducer(self._properties, default_value_schema=value_schema)
        elif (self._config.get(self._SCHEMA_VALUE_FILE)== None):
            key_schema = avro.load(self._config.get(self._SCHEMA_KEY_FILE))
            producer = AvroProducer(self._properties, default_key_schema=key_schema)
        else:
            value_schema = avro.load(self._config.get(self._SCHEMA_VALUE_FILE))
            key_schema = avro.load(self._config.get(self._SCHEMA_KEY_FILE))
            producer = AvroProducer(self._properties, default_key_schema=key_schema, default_value_schema=value_schema)
        return producer

    def send (self, key=None, value=None, flush=False):
        topic = self._config.get(self._TOPIC)        
        source_type = self._config.get(self._SOURCE_TYPE)
        #TODO revisar
        #(key,value)=self.utils.getKeyValue(key,value,self._config)
        #TODO partition
        if (source_type==self._SOURCE_TYPE_FOLDER):
            self.sendFolder(topic,value,key)
        elif (source_type==self._SOURCE_TYPE_FILE):
            self.sendFile(topic,value,key)
        else:
            self.sendValue(topic,value,key,flush)

    def sendFolder (self, topic, value, key=None, partition=None):
        '''
        get all the files contained in a folder
        '''
        files = self.utils.getFiles(value)        
        for f in files:
            self.sendFile(topic,f, key,partition)

    def sendFile (self, topic, value, key=None, partition=None):
        '''
        read file and send to broker line by line
        '''
        with open(value, 'r') as f:
            for line in f:
                self.sendValue(topic, line, key, partition,flush=False)
            self.flush(True)

        
    
    def sendValue (self, topic, value, key=None, partition=None,flush=True):   
        try:  
            self.producer.produce(topic=topic,value=value,key=key,callback=(None, self.utils.delivery_callback)[self._config.get(self._DEBUG_MODE)])
            self.flush(flush)
            #TODO handle broker down
        except KafkaException as e:
            print("An error was encountered while producing a kafka message: %s", str(e.args[0]))
            print("Retrying producing kafka message ...")
            #time.sleep(retry_interval)

    def flush(self, flush):
        if (flush):
            if len(self.producer)>0:
                sys.stderr.write('%% Waiting for %d deliveries\n' % len(self.producer))
            self.producer.flush()
if __name__ == "__main__":
    #TODO dar una vuelta 
    p = KafkaProducer('user.json')
    p.send()
