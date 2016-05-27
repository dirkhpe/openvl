import json
import logging
import os
import sys
import ckanapi
from lib import my_env


def package_list():
    """
    URL: http://ckan-001.corve.openminds.be/api/3/action/package_list
    Return the metadata of a dataset (package) and its resources.
    :return:
    """
    try:
        res = ckan_conn.action.package_list()
    except:
        e = sys.exc_info()[1]
        ec = sys.exc_info()[0]
        log_msg = "Package List not successful %s %s"
        logging.error(log_msg, e, ec)
        return False
    return res


def package_show(package_name, ds_dir):
    """
    URL: http://ckan-001.corve.openminds.be/api/3/action/package_show?
    id=dmow-ind003-filezwaarte_op_het_hoofdwegennet&include_tracking=True
    Return the metadata of a dataset (package) and its resources.
    :param package_name:
    :param ds_dir: Dataset Directory, where the metadata files will be written.
    :return:
    """
    if package_name is None:
        print('Package Name needs to be defined!')
    else:
        try:
            res = ckan_conn.action.package_show(id=package_name, include_tracking=1)
        except:
            e = sys.exc_info()[1]
            ec = sys.exc_info()[0]
            log_msg = "Package Show not successful %s %s"
            logging.error(log_msg, e, ec)
            return False
        else:
            outfile = os.path.join(ds_dir, res['name'] + ".json")
            f = open(outfile, 'w')
            # print("Organization: " + res['organization']['name'])
            f.write(json.dumps(res, indent=4))
            logging.info("Output written to " + outfile)
            f.close()


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


# Get ini-file first.
projectname = 'openvl'
modulename = my_env.get_modulename(__file__)
config = my_env.get_inifile(projectname, __file__)
# Now configure logfile
my_env.init_loghandler(config, modulename)
logging.info('Start Application')
ckan_conn = get_ckan_conn()
logdir = config['Main']['logdir']
dataset_dir = config['Main']['ds_dir']
pl = package_list()
cnt = 0
total = 0
for ds_id in pl:
    package_show(ds_id, dataset_dir)
    total += 1
    cnt += 1
    if cnt >= 10:
        print("{total} datasets processed".format(total=total))
        cnt = 0
# set_pkg_private('18d67cd3-a9c5-45aa-b5bc-9be94c6cb258')
# package_list()
logging.info('End Application')
