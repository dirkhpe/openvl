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
        logging.error('Package Name needs to be defined!')
        return False
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
            logging.debug("Output written to " + outfile)
            f.close()
            return True


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
projectname = 'opennl'
modulename = my_env.get_modulename(__file__)
config = my_env.get_inifile(projectname, __file__)
# Now configure logfile
my_env.init_loghandler(config, modulename)
logging.info('Start Application')
ckan_conn = get_ckan_conn()
logdir = config['Main']['logdir']
dataset_dir = config['Main']['ds_dir']
pl = package_list()
# First remove all datasets in directory
filelist = [file for file in os.listdir(dataset_dir) if os.path.splitext(file)[1] == ".json"]
logging.info("Remove all files from directory {dsdir}".format(dsdir=dataset_dir))
for idx, file in enumerate(filelist):
    os.remove(os.path.join(dataset_dir, file))
# Then collect current datasets
logging.info("Start to collect datasets")
for idx, ds_id in enumerate(pl):
    package_show(ds_id, dataset_dir)
    if idx % 100 == 0:
        logging.info("{idx} datasets processed".format(idx=idx))
logging.info('End Application')
