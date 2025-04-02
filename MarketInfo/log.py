import datetime
import logging
import os


class Log:
    def __init__(self):
        log_dir = os.path.dirname(os.path.abspath(__file__)) + '/logs/'
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
        log_path = log_dir + datetime.date.today().strftime('%Y-%m-%d') + '_Simple' + '.log'
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d >> %(message)s',
            datefmt='%Y-%m-%dT %H:%M:%S+0800',
            handlers=[logging.StreamHandler(), logging.FileHandler(filename=log_path)])
        self.log = logging.getLogger(name='MarketInfo')


log = Log().log
