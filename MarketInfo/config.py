from configparser import ConfigParser
import os

pro_path = os.path.split(os.path.abspath(__file__))[0]

setting_path = os.path.join(pro_path, 'main.ini')

config = ConfigParser()
config.read(setting_path, encoding='utf-8')

OceanBase = {}
for k, v in config.items('OceanBase'):
    if k == 'jar':
        OceanBase[k] = os.path.join(os.path.join(pro_path, 'driver'), v)
    else:
        OceanBase[k] = v
