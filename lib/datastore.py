"""
This class consolidates functions related to the local datastore.
"""

import logging
import sqlite3
import sys


class DataStore:

    def __init__(self, config):
        """
        Method to instantiate the class in an object for the datastore.
        :param config object, to get connection parameters.
        :return: Object to handle datastore commands.
        """
        logging.debug("Initializing Datastore object")
        self.config = config
        self.dbConn, self.cur = self._connect2db()
        return

    def _connect2db(self):
        """
        Internal method to create a database connection and a cursor. This method is called during object
        initialization.
        Note that sqlite connection object does not test the Database connection. If database does not exist, this
        method will not fail. This is expected behaviour, since it will be called to create databases as well.
        :return: Database handle and cursor for the database.
        """
        logging.debug("Creating Datastore object and cursor")
        db = self.config['Main']['db']
        try:
            db_conn = sqlite3.connect(db)
        except:
            e = sys.exc_info()[1]
            ec = sys.exc_info()[0]
            log_msg = "Error during connect to database: %s %s"
            logging.error(log_msg, e, ec)
            return
        else:
            logging.debug("Datastore object and cursor are created")
            return db_conn, db_conn.cursor()

    def close_connection(self):
        """
        Method to close the Database Connection.
        :return:
        """
        logging.debug("Close connection to database")
        try:
            self.dbConn.close()
        except:
            e = sys.exc_info()[1]
            ec = sys.exc_info()[0]
            log_msg = "Error during close connect to database: %s %s"
            logging.error(log_msg, e, ec)
            return
        else:
            return

    def create_tables(self):
        # Create table
        query = """
        CREATE TABLE dataset
            (id text primary key,
             creator_user_id text,
             owner_org text,
             name text unique,
             title text,
             notes text,
             author text,
             author_email text,
             maintainer text,
             maintainer_email text,
             lod_stars text,
             theme_facet text,
             resources integer,
             tags integer,
             groups integer,
             metadata_created text,
             metadata_modified text,
             tracking_summary_recent integer,
             tracking_summary_total integer)
        """
        try:
            self.dbConn.execute(query)
        except:
            e = sys.exc_info()[1]
            ec = sys.exc_info()[0]
            log_msg = "Error during query execution - Attribute_action: %s %s"
            logging.error(log_msg, e, ec)
            return False
        logging.info("Table dataset is build.")
        return True

    def remove_tables(self):
        query = 'DROP TABLE IF EXISTS dataset'
        try:
            self.dbConn.execute(query)
        except:
            e = sys.exc_info()[1]
            ec = sys.exc_info()[0]
            log_msg = "Error during query execution: %s %s"
            logging.error(log_msg, e, ec)
            return False
        else:
            logging.info("Drop table dataset")
            return True

    def create_table_user_data(self):
        # Create table
        query = """
        CREATE TABLE user_data
            (id text primary key,
             name text,
             fullname text,
             display_name text,
             about text,
             state text,
             number_of_edits integer,
             number_created_packages integer,
             created text)
        """
        try:
            self.dbConn.execute(query)
        except:
            e = sys.exc_info()[1]
            ec = sys.exc_info()[0]
            log_msg = "Error during query execution - Attribute_action: %s %s"
            logging.error(log_msg, e, ec)
            return False
        logging.info("Table user_data is build.")
        return True

    def remove_table_user_data(self):
        query = 'DROP TABLE IF EXISTS user_data'
        try:
            self.dbConn.execute(query)
        except:
            e = sys.exc_info()[1]
            ec = sys.exc_info()[0]
            log_msg = "Error during query execution: %s %s"
            logging.error(log_msg, e, ec)
            return False
        else:
            logging.info("Drop table user_data")
            return True

    def get_columns(self, tablename):
        """
        This method will get column names for the specified table.
        :param tablename:
        :return:
        """
        cols = self.cur.execute("PRAGMA table_info({tn})".format(tn=tablename))
        return [col[1] for col in cols]

    def insert_row(self, tablename, rowdict):
        columns = ", ".join(rowdict.keys())
        values_template = ", ".join(["?"] * len(rowdict.keys()))
        query = "insert into  {tn} ({cols}) values ({vt})".format(tn=tablename, cols=columns, vt=values_template)
        values = tuple(rowdict[key] for key in rowdict.keys())
        self.dbConn.execute(query, values)
        self.dbConn.commit()
        return
