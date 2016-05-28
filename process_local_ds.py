import dateutil.parser
import json
import logging
import os
from lib import my_env
from lib.datastore import DataStore


def handle_ds(filepath, ds, cols):
    """
    This method will read the dataset info that is locally available.
    :param filepath:
    :param ds: Datastore pointer
    :param cols: List of fields to check for.
    :return:
    """
    f = open(filepath)
    res = json.load(f)
    rowdict = {}
    for col in cols:
        if col in res:
            if col in ["resources", "tags"]:
                rowdict[col] = len(res[col])
            elif col in ["metadata_created", "metadata_modified"]:
                rowdict[col] = dateutil.parser.parse(res[col]).strftime("%d/%m/%Y %H:%M:%S")
            elif res[col]:
                rowdict[col] = res[col]
    ds.insert_row('dataset', rowdict)
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
    scandir = config['Main']['ds_dir']
    ds = DataStore(config)
    # Reset Table
    ds.remove_tables()
    ds.create_tables()
    cols = ds.get_columns('dataset')
    print("Cols: {cols}".format(cols=cols))
    filelist = [file for file in os.listdir(scandir) if os.path.splitext(file)[1] == ".json"]
    for file in filelist:
        handle_ds(os.path.join(scandir, file), ds, cols)
    logging.info('End Application')
