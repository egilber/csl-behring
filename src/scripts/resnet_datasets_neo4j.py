import psycopg2 as ps2
import os
import json
import argparse

import pandas as pd
import numpy as np
import csv
from dotenv import load_dotenv
from typing import Any, Optional, List, Tuple, NoReturn

load_dotenv()
database_name = 'resnet18'

directional_sql_query = f"SELECT \
control.id, inkey[1], controltype,  string_agg(distinct(effect), ', '), string_agg(distinct(mechanism), ', '), \
num_refs, outkey[1] , reference.id, string_agg(distinct(biomarkertype), ', ') ,\
string_agg(distinct(celllinename), ', '), string_agg(distinct(celltype), ', '), string_agg(distinct(changetype), ', '),\
string_agg(distinct(organ), ', '), string_agg(distinct(organism), ', '), string_agg(distinct(quantitativetype), ', '), \
string_agg(distinct(tissue), ', '), string_agg(distinct(nct_id), ', '),  string_agg(distinct(phase), ', ') \
FROM {database_name}.control, {database_name}.reference \
WHERE control.id = reference.id and inkey[1] is not null and outkey[1] is not null \
GROUP BY control.id, inkey[1], controltype, num_refs, outkey[1], reference.id"

bi_directional_sql_query = f"SELECT \
control.id, inkey[1], inoutkey, controltype, relationship, string_agg(distinct(effect), ', '), \
string_agg(distinct(mechanism), ', '), num_refs, outkey[1], reference.id, string_agg(distinct(biomarkertype), ', '), \
string_agg(distinct(celllinename), ', '), string_agg(distinct(celltype), ', '), string_agg(distinct(changetype), ', '),\
string_agg(distinct(organ), ', '), string_agg(distinct(organism), ', '), string_agg(distinct(quantitativetype), ', '), \
string_agg(distinct(tissue), ', ') \
FROM {database_name}.control, {database_name}.reference \
WHERE control.id = reference.id and inkey[1] is null and outkey[1] is null \
GROUP BY control.id, controltype, reference.id"

attributes_sql_query = f"SELECT  \
id, inkey[1], attributes, relationship, outkey[1] from {database_name}.control \
WHERE (control.id = control.attributes)"

node_sql_query = 'select id, name, nodetype from resnet18.node where id is not null and name is not null and nodetype \
 is not null'


class CreateDatasets:

    def __init__(self, base_path):
        """
        Initialize the CreateDatasets object with the base path for data storage.

        This class is designed to handle the creation and management of datasets
        for a database, including setting paths for various datasets such as directional,
        bidirectional, attributes, and nodes datasets. It also stores database connection
        information.

        Args:
            base_path (str): The base file path where dataset files will be stored.

        Attributes:
            db_name (Optional[str]): Name of the database, initially None.
            db_user (Optional[str]): Username for database access, initially None.
            db_host (Optional[str]): Host address of the database, initially None.
            db_pwd (Optional[str]): Password for database access, initially None.
            base_path (str): Base path for storing dataset files.
            directional_ds (Optional[str]): Path to the directional dataset, initially None.
            bi_directional_ds (Optional[str]): Path to the bidirectional dataset, initially None.
            attributes_ds (Optional[str]): Path to the attributes dataset, initially None.
            nodes_ds (Optional[str]): Path to the nodes dataset, initially None.
        """
        self.db_name: Optional[str] = None
        self.db_user: Optional[str] = None
        self.db_host: Optional[str] = None
        self.db_pwd: Optional[str] = None
        self.base_path: str = base_path
        self.directional_ds: Optional[str] = None
        self.bi_directional_ds: Optional[str] = None
        self.attributes_ds: Optional[str] = None
        self.nodes_ds: Optional[str] = None

    def get_sqldb_creds(self) -> Tuple[str, str, str, str]:
        """
        Retrieve SQL database credentials from environment variables.

        Attempts to read the database credentials (name, user, host IP, and password)
        from the environment variables. Raises an exception if any of the required
        credentials are not provided via environment variables, to prevent proceeding
        without proper configuration.

        Returns:
            Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]: A tuple containing
            the database name, user, host, and password. Each element of the tuple is either a
            string or None if the environment variable is not set.

        Raises:
            EnvironmentError: If any of the required environment variables are not set.
        """
        self.db_name = os.getenv('DB_NAME')
        self.db_user = os.getenv('DB_USER')
        self.db_host = os.getenv('DB_HOST_IP')
        self.db_pwd = os.getenv('DB_PWD')

        if None in {self.db_name, self.db_user, self.db_host, self.db_pwd}:
            missing = [var for var, value in
                       [('DB_NAME', self.db_name), ('DB_USER', self.db_user), ('DB_HOST_IP', self.db_host),
                        ('DB_PWD', self.db_pwd)] if value is None]
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")
        return self.db_name, self.db_user, self.db_host, self.db_pwd

    def create_connection_to_postgresql_db(self) -> ps2.extensions.connection:
        """
        Creates and returns a connection to the PostgreSQL database using credentials
        retrieved via `get_sqldb_creds` method.

        This method establishes a connection to the PostgreSQL database using the credentials
        retrieved from the `get_sqldb_creds` method. It facilitates the connection to the database
        for subsequent operations.

        Returns:
            ps2.extensions.connection: A connection object to the PostgreSQL database.

        Raises:
            psycopg2.DatabaseError: An error from psycopg2 when the connection cannot be established.
        """
        self.db_name, self.db_user, self.db_host, self.db_pwd = CreateDatasets.get_sqldb_creds()
        conn = ps2.connect(
            host=self.db_host,
            database=self.db_name,
            user=self.db_user,
            password=self.db_pwd)
        return conn

    def _update_config_file(self, key: str, value: str) -> NoReturn:
        """
        Updates the configuration file `file_paths.json` located at `self.base_path` with the
        provided key-value pair. If the configuration file does not exist, this method creates one.

        This method is responsible for managing the persistent storage of configuration data,
        allowing dynamic changes to the dataset file paths or other configuration parameters
        to be retained across executions.

        Args:
            key (str): The key to be added or updated in the configuration file.
            value (str): The value to be assigned to the key in the configuration file.

        Raises:
            IOError: If there is an error in opening, reading, or writing to the file.
        """
        config_file = os.path.join(self.base_path, 'file_paths.json')
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    file_paths = json.load(f)
            else:
                file_paths = {}

            file_paths[key] = value

            with open(config_file, 'w') as f:
                json.dump(file_paths, f)
        except IOError as e:
            raise IOError(f"Error accessing or modifying the configuration file: {e}")

    def _load_dataset_file_paths(self) -> NoReturn:
        """
        Load dataset file paths from the configuration file `file_paths.json` located
        at `self.base_path`.

        Returns:
            dict: A dictionary containing the file paths loaded from the configuration file.
                  The keys are the identifiers of the file paths, and the values are the
                  corresponding paths.

        Raises:
            IOError: If there is an error in opening/reading the file.
            json.JSONDecodeError: If there is an error decoding the JSON data from the file.
        """
        config_file = os.path.join(self.base_path, 'file_paths.json')
        try:
            with open(config_file, 'r') as f:
                file_paths = json.load(f)
        except IOError as e:
            raise IOError(f"Error opening/reading file: {e}")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Error decoding JSON data from file: {e}")
        self.directional_ds = file_paths.get('directional_ds')
        self.bi_directional_ds = file_paths.get('bi_directional_ds')
        self.attributes_ds = file_paths.get('attributes_ds')

    def create_directional_ds(self, file_name: str) -> NoReturn:
        """
        Creates a Resnet directional relationship dataset file from a SQL query execution, writes it to a specified
        file, and updates the configuration to include this dataset path.

        This method connects to a PostgreSQL database, executes a predefined SQL query to fetch
        data for the directional relationship dataset, and writes the fetched records to a file in the specified
        location with a `.txt` extension. It updates the configuration file with the path to this
        newly created dataset.

        Args:
            file_name (str): The base name of the file where the dataset will be saved. If the
                             file name does not end with '.txt', the extension will be appended.

        Raises:
            IOError: If there is an error in opening/writing to the output file.
            psycopg2.DatabaseError: If there is an error connecting to the database or executing
                                    the SQL query.
        """
        output_path = os.path.join(self.base_path, file_name)
        if not output_path.endswith('.txt'):
            output_path += '.txt'
        conn = CreateDatasets.create_connection_to_postgresql_db()
        try:
            with conn.cursor() as cur:
                with open(output_path, 'w', encoding="utf-8") as f:
                    cur.execute(directional_sql_query)
                    csv_writer = csv.writer(f, delimiter='|')
                    for record in cur.fetchall():
                        csv_writer.writerow(record)

            self._update_config_file('directional_ds', output_path)
        finally:
            conn.close()

    def create_bi_directional_ds(self, file_name):
        """
        Creates a Resnet bi-directional relationship dataset file from SQL query execution, writes it to the specified
        file, and updates the configuration to include this new dataset.

        Connects to a PostgreSQL database, executes a predefined SQL query for fetching
        bi-directional dataset data, and writes the fetched records into a file at the specified
        location with a '.txt' extension. It then updates the configuration file with the path
        to this newly created dataset.

        Args:
            file_name (str): The base name of the file where the dataset will be saved. If the
                             file name does not end with '.txt', the extension is appended.

        Raises:
            IOError: If there is an issue opening/writing to the output file.
            psycopg2.DatabaseError: If an error occurs connecting to the database or executing
                                    the SQL query.
        """
        output_path = os.path.join(self.base_path, file_name)
        if not output_path.endswith('.txt'):
            output_path += '.txt'
        conn = CreateDatasets.create_connection_to_postgresql_db()
        try:
            with conn.cursor() as cur:
                with open(output_path, 'w', encoding="utf-8") as f:
                    cur.execute(bi_directional_sql_query)
                    csv_writer = csv.writer(f, delimiter='|')
                    for record in cur.fetchall():
                        csv_writer.writerow(record)

            self._update_config_file('bi_directional_ds', output_path)
        finally:
            conn.close()

    def create_attributes_ds(self, file_name: str) -> NoReturn:
        """
        Creates a Resnet attributes dataset file from SQL query execution, writes it to the specified
        file, and updates the configuration to include this new dataset.

        Connects to a PostgreSQL database, executes a predefined SQL query for fetching
        attributes data, and writes the fetched records into a file at the specified
        location with a '.txt' extension. It then updates the configuration file with the path
        to this newly created dataset.

        Args:
            file_name (str): The base name of the file where the dataset will be saved. If the
                             file name does not end with '.txt', the extension is appended.

        Raises:
            IOError: If there is an issue opening/writing to the output file.
            psycopg2.DatabaseError: If an error occurs connecting to the database or executing
                                    the SQL query.
        """
        output_path = os.path.join(self.base_path, file_name)
        if not output_path.endswith('.txt'):
            output_path += '.txt'
        conn = CreateDatasets.create_connection_to_postgresql_db()
        try:
            with conn.cursor() as cur:
                with open(output_path, 'w', encoding="utf-8") as f:
                    cur.execute(attributes_sql_query)
                    csv_writer = csv.writer(f, delimiter='|')
                    for record in cur.fetchall():
                        csv_writer.writerow(record)

            self._update_config_file('attributes_ds', output_path)
        finally:
            conn.close()

    def create_nodes_ds(self, file_name: str) -> NoReturn:
        """
        Creates a nodes dataset file from SQL query execution, writes it to the specified
        file, and updates the configuration to include this new dataset.

        Connects to a PostgreSQL database, executes a predefined SQL query for fetching
        nodes data, and writes the fetched records into a file at the specified
        location with a '.txt' extension. It then updates the configuration file with the path
        to this newly created dataset.

        Args:
            file_name (str): The base name of the file where the dataset will be saved. If the
                             file name does not end with '.txt', the extension is appended.

        Raises:
            IOError: If there is an issue opening/writing to the output file.
            psycopg2.DatabaseError: If an error occurs connecting to the database or executing
                                    the SQL query.
        """
        output_path = os.path.join(self.base_path, file_name)
        if not output_path.endswith('.txt'):
            output_path += '.txt'
        conn = CreateDatasets.create_connection_to_postgresql_db()
        try:
            with conn.cursor() as cur:
                with open(output_path, 'w', encoding="utf-8") as f:
                    cur.execute(node_sql_query)
                    csv_writer = csv.writer(f, delimiter='|')
                    for record in cur.fetchall():
                        csv_writer.writerow(record)

            self._update_config_file('nodes_ds', output_path)
        finally:
            conn.close()


def main():
    """
    Entry point of the script that processes datasets based on command line arguments.

    The function sets up a command line parser to specify which dataset creation methods to
    call, and processes datasets accordingly based on the specified command line arguments.
    It allows the user to select which method to use for dataset creation, define the base
    path for input/output files, and specify the filename for the dataset to be saved.

    Usage:
        python script_name.py --method create_directional_dataset --file_name example.txt

    Args:
        --method (str): One or more dataset methods to call, from the following options:
                        'create_directional_dataset', 'create_bi_directional_dataset',
                        'create_attributes_dataset', 'create_nodes_ds'.
        --base_path (str): Base directory path where datasets are read from and saved to.
                           Defaults to './data/processed/'.
        --file_name (str): Name of the file to save the dataset to. This argument is required.
    """
    parser = argparse.ArgumentParser(description='Choose the methods to call.')
    parser.add_argument('--method', type=str, nargs='+', choices=['create_directional_dataset',
                                                                  'create_bi_directional_dataset',
                                                                  'create_attributes_dataset',
                                                                  'create_nodes_ds'])

    parser.add_argument('--base_path', type=str, default='./data/processed/', help='The base path for the input and '
                                                                                   'output files.')
    parser.add_argument('--file_name', type=str, required=True,
                        help='The name of the parquet file to save the dataset to.')
    args = parser.parse_args()

    cd = CreateDatasets(base_path=args.base_path)
    for method in args.method:
        if method == 'create_directional_dataset':
            cd.create_directional_ds(args.file_name)
        elif method == 'create_bi_directional_dataset':
            cd.create_bi_directional_ds(args.file_name)
        elif method == 'create_attributes_dataset':
            cd.create_attributes_ds(args.file_name)
        elif method == 'create_nodes_ds':
            cd.create_nodes_ds(args.file_name)
        else:
            print("Invalid method choice. Use --help for more information.")


if __name__ == "__main__":
    main()
