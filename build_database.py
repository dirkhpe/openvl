"""
This script will rebuild the database from scratch. It should run only once during production
and many times during development.
"""

import logging
from lib.datastore import DataStore
from lib import my_env

if __name__ == "__main__":
    # Initialize Environment
    projectname = "openvl"
    modulename = my_env.get_modulename(__file__)
    config = my_env.get_inifile(projectname, __file__)
    my_env.init_loghandler(config, modulename)
    ds = DataStore(config)
    logging.info('Start Application')
    ds.remove_tables()
    ds.create_tables()
    logging.info('End Application')
