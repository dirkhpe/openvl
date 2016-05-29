import ckanapi
import dateutil.parser
import logging
import sys
from lib import my_env
from lib.datastore import DataStore


def user_list():
    """
    URL: http://ckan-001.corve.openminds.be/api/3/action/user_list
    Return the metadata of the users.
    :return: User data.
    """
    try:
        res = ckan_conn.action.user_list()
    except:
        e = sys.exc_info()[1]
        ec = sys.exc_info()[0]
        log_msg = "User List not successful %s %s"
        logging.error(log_msg, e, ec)
        return False
    return res


def get_ckan_conn():
    """
    Configure the connection to ckan Open Data Platform.
    :return:
    """
    logging.debug("Setup connection to ckan Server")
    url = config['CKANServer']['url']
    try:
        ckanconn = ckanapi.RemoteCKAN(url)
    except:
        e = sys.exc_info()[1]
        ec = sys.exc_info()[0]
        log_msg = "Connect to RemoteCKAN not successful %s %s"
        logging.critical(log_msg, e, ec)
        sys.exit(1)
    return ckanconn


def handle_ul(ul, cols):
    """
    This method will handle the user_list information.
    :param ul: User List information. List of user dictionaries
    :param cols: List of fields to check for.
    :return:
    """
    for user in ul:
        rowdict = {}
        for col in cols:
            if col in user:
                if col in ["created"]:
                    rowdict[col] = dateutil.parser.parse(user[col]).strftime("%d/%m/%Y %H:%M:%S")
                elif user[col]:
                    rowdict[col] = user[col]
        dbConn.insert_row('user_data', rowdict)
    return


if __name__ == "__main__":
    # Get ini-file first.
    projectname = 'openvl'
    modulename = my_env.get_modulename(__file__)
    config = my_env.get_inifile(projectname, __file__)
    # Now configure logfile
    my_env.init_loghandler(config, modulename)
    logging.info('Start Application')
    logdir = config['Main']['logdir']
    dbConn = DataStore(config)
    # Reset Table
    dbConn.remove_table_user_data()
    dbConn.create_table_user_data()
    columns = dbConn.get_columns('user_data')
    ckan_conn = get_ckan_conn()
    ul = user_list()
    handle_ul(ul, columns)
    logging.info('End Application')
