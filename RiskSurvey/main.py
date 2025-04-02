from Simple.utils.configuration_table import Config
from Simple.core.foreign_currency import foreign
from Simple.core.commodity import commodity
from Simple.utils.report_data import start
from Simple.core.interest_rate import ir, ir_cls
from Simple.core.option import option
from Simple.utils.connect import OceanBaseConnect
from Simple.utils.log import log
import sys


def main(partition_key):
    log.info('Simple start ...')
    connect = OceanBaseConnect()
    config = Config('CNY', partition_key, connect=connect, write_to_database=True)

    ir(config)
    ir_cls(config)
    foreign(config)
    option(config)
    commodity(config)
    start(partition_key)
    log.info('Simple over ...')
    sys.exit(0)


if __name__ == '__main__':
    partition_key = sys.argv[1]
    main(partition_key)
