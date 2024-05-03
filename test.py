import configparser
from abc import ABC
# config = configparser.ConfigParser()
# config['IO'] = {'source': 'sql', 'compression': 'yes'}


class iConfig(ABC):
    def set():
        pass

class Config(iConfig, configparser.ConfigParser):
    def __init__(self):
        super().__init__(self)

    def set(self):
        pass

class ModellerArgs:
    def __init__(self):
        self._config = []


config = Config()
config['test'] = {'Test': 'pd_odr_mev.csv'}
# test = ModellerArgs()
# test.config = {'ODR_MEV': 'pd_odr_mev.csv'}
# print(test.config)
with open('config.ini', 'w') as w:
    config.write(w)