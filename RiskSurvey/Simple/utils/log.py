import logging
import os
from Simple.utils.utils import get_current_time


class Log:

    def __init__(self):
        log_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/logs/'
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
        log_path = log_dir + get_current_time().split(' ')[0] + '_Simple' + '.log'
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d >> %(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S+0800',
            handlers=[logging.StreamHandler(), logging.FileHandler(filename=log_path)])
        self.log = logging.getLogger(name='Simple')


log = Log().log
