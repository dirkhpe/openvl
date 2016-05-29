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
            if col in ["resources", "tags", "groups"]:
                rowdict[col] = len(res[col])
            elif col in ["metadata_created", "metadata_modified"]:
                rowdict[col] = dateutil.parser.parse(res[col]).strftime("%d/%m/%Y %H:%M:%S")
            elif res[col]:
                rowdict[col] = res[col]
    # Special handling for tracking_summary
    if res["tracking_summary"]:
        rowdict["tracking_summary_total"] = res["tracking_summary"]["total"]
        rowdict["tracking_summary_recent"] = res["tracking_summary"]["recent"]
    ds.insert_row('dataset', rowdict)
    return


if __name__ == "__main__":
    # Get ini-file first.
    projectname = 'opennl'
    modulename = my_env.get_modulename(__file__)
    config = my_env.get_inifile(projectname, __file__)
    # Now configure logfile
    my_env.init_loghandler(config, modulename)
    logging.info('Start Application')
    logdir = config['Main']['logdir']
    dataset_dir = config['Main']['ds_dir']
    dbConn = DataStore(config)
    # Reset Table
    dbConn.remove_tables()
    dbConn.create_tables()
    columns = dbConn.get_columns('dataset')
    print("Cols: {cols}".format(cols=columns))
    filelist = [file for file in os.listdir(dataset_dir) if os.path.splitext(file)[1] == ".json"]
    for idx, file in enumerate(filelist):
        handle_ds(os.path.join(dataset_dir, file), dbConn, columns)
        if idx % 100 == 0:
            logging.info("{idx} files processed.".format(idx=idx))
    logging.info('End Application')
