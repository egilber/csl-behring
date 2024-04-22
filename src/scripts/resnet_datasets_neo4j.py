import psycopg2 as ps2
from ps2.extensions import connection as PsycopgConnection

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
        Initializes a new instance of the CreateDatasets class which handles the creation and management
        of datasets for a database.

        This constructor sets up the storage paths for various types of datasets and initializes properties
        for database connectivity, which are to be defined later if database interactions are required.

        Args:
            base_path (str): The base file path where dataset files will be stored.

        Properties:
            base_path (str): Base path for storing dataset files.
            db_name (Optional[str]): Name of the database, if applicable. Initially None.
            db_user (Optional[str]): Username for database access, if applicable. Initially None.
            db_host (Optional[str]): Host address of the database, if applicable. Initially None.
            db_pwd (Optional[str]): Password for database access, if applicable. Initially None.
            directional_ds (Optional[str]): Path to the directional dataset, initially None.
            bi_directional_ds (Optional[str]): Path to the bidirectional dataset, initially None.
            attributes_ds (Optional[str]): Path to the attributes dataset, initially None.
            nodes_ds (Optional[str]): Path to the nodes dataset, initially None.
        """
        self.base_path: str = base_path
        self.db_name: Optional[str] = None
        self.db_user: Optional[str] = None
        self.db_host: Optional[str] = None
        self.db_pwd: Optional[str] = None
        self.directional_ds: Optional[str] = None
        self.bi_directional_ds: Optional[str] = None
        self.attributes_ds: Optional[str] = None
        self.nodes_ds: Optional[str] = None

    def get_sqldb_creds(self) -> Tuple[str, str, str, str]:
        """
        Retrieve SQL database credentials from environment variables.

        Attempts to read the database credentials (name, user, host IP, and password)
        from the environment variables. This method will raise an exception if any of
        the required credentials are not provided via environment variables, to ensure
        the application does not proceed without proper configuration.

        Returns:
            Tuple[str, str, str, str]: A tuple containing the database name, user, host IP,
            and password. It guarantees that all elements of the tuple are non-None strings,
            as it raises an exception if any are missing.

        Raises:
            EnvironmentError: If any of the required environment variables (DB_NAME, DB_USER,
            DB_HOST_IP, DB_PWD) are not set.
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

    def create_connection_to_postgresql_db(self) -> PsycopgConnection:
        """
        Creates and returns a connection to the PostgreSQL database using credentials
        retrieved via the `get_sqldb_creds` method.

        This method establishes a connection to the PostgreSQL database using the credentials
        retrieved from the `get_sqldb_creds` method. It ensures the connection setup is handled
        correctly and facilitates database interactions for subsequent operations.

        Returns:
            PsycopgConnection: A connection object to the PostgreSQL database, which allows for
            interacting with the database.

        Raises:
            psycopg2.DatabaseError: An error from psycopg2 when the connection cannot be established,
            which may occur due to incorrect credentials or network issues.
        """
        self.db_name, self.db_user, self.db_host, self.db_pwd = self.get_sqldb_creds()
        conn = ps2.connect(
            host=self.db_host,
            database=self.db_name,
            user=self.db_user,
            password=self.db_pwd)
        return conn

    def _update_config_file(self, key: str, value: str) -> None:
        """
        Updates the configuration file `file_paths.json` at `self.base_path` with the provided key-value pair.
        If the configuration file does not exist, it is created. This method ensures that configuration changes
        are persisted, allowing for the dynamic modification of dataset file paths or other configuration parameters.

        Args:
            key (str): The key to be added or updated in the configuration file.
            value (str): The value to be assigned to the key in the configuration file.

        Raises:
            IOError: If there is an error in opening, reading, or writing to the file, encapsulating issues like
            file not found, permission issues, or failures in reading/writing data.
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
                json.dump(file_paths, f, indent=4)
        except IOError as e:
            raise IOError(f"Error accessing or modifying the configuration file: {e}")

    def _load_dataset_file_paths(self) -> None:
        """
        Loads dataset file paths from the configuration file `file_paths.json` located at `self.base_path`.
        This method updates the instance attributes with the paths stored in the configuration file.

        This method does not return any values but updates the object's state based on the configuration.

        Raises:
            IOError: If there is an error in opening or reading the file, encapsulating issues such as file not
                     found or read permissions.
            json.JSONDecodeError: If there is an error decoding the JSON data from the file, indicating issues
                                  such as improper file format or corruption.
        """
        config_file = os.path.join(self.base_path, 'file_paths.json')
        try:
            with open(config_file, 'r') as f:
                file_paths = json.load(f)
        except IOError as e:
            raise IOError(f"Error opening/reading file at {config_file}: {e}") from e
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Error decoding JSON data from file at {config_file}: {e}") from e

        # Update instance attributes with loaded paths
        self.directional_ds = file_paths.get('directional_ds')
        self.bi_directional_ds = file_paths.get('bi_directional_ds')
        self.attributes_ds = file_paths.get('attributes_ds')
        self.nodes_ds = file_paths.get('nodes_ds', None)  # Added safe handling for 'nodes_ds'

    def create_directional_ds(self, file_name: str) -> None:
        """
        Creates a Resnet directional relationship dataset file from a SQL query execution, writes it to a specified
        file, and updates the configuration to i    nclude this dataset path.

        This method connects to a PostgreSQL database, executes a predefined SQL query to fetch
        data for the directional relationship dataset, and writes the fetched records to a file in the specified
        location with a `.txt` extension. It updates the configuration file with the path to this
        newly created dataset.

        Args:
            file_name (str): The base name of the file where the dataset will be saved. The '.txt' extension
                             will be appended if not already present.

        Raises:
            IOError: If there is an error in opening/writing to the output file.
            psycopg2.DatabaseError: If there is an error connecting to the database or executing
                                    the SQL query.
        """
        output_path = os.path.join(self.base_path, file_name)
        if not output_path.endswith('.txt'):
            output_path += '.txt'
        conn = self.create_connection_to_postgresql_db()
        try:
            with conn.cursor() as cur:
                with open(output_path, 'w', encoding="utf-8") as f:
                    cur.execute(directional_sql_query)
                    csv_writer = csv.writer(f, delimiter='|')
                    for record in cur.fetchall():
                        csv_writer.writerow(record)
            self._update_config_file('directional_ds', output_path)
        except ps2.DatabaseError as e:
            raise ps2.DatabaseError(f"Database operation failed: {e}") from e
        except IOError as e:
            raise IOError(f"Failed to write to file at {output_path}: {e}") from e
        finally:
            if conn:
                conn.close()

    def create_bi_directional_ds(self, file_name: str) -> None:
        """
        Creates a Resnet bi-directional relationship dataset file from SQL query execution, writes it to the specified
        file, and updates the configuration to include this new dataset.

        Connects to a PostgreSQL database, executes a predefined SQL query for fetching
        bi-directional dataset data, and writes the fetched records into a file at the specified
        location with a '.txt' extension. It then updates the configuration file with the path
        to this newly created dataset.

        Args:
            file_name (str): The base name of the file where the dataset will be saved. The '.txt'
                             extension is appended if not already present.

        Raises:
            IOError: If there is an issue opening/writing to the output file.
            psycopg2.DatabaseError: If an error occurs connecting to the database or executing
                                    the SQL query.
        """
        output_path = os.path.join(self.base_path, file_name)
        if not output_path.endswith('.txt'):
            output_path += '.txt'
        conn = self.create_connection_to_postgresql_db()
        try:
            with conn.cursor() as cur:
                with open(output_path, 'w', encoding="utf-8") as f:
                    cur.execute(bi_directional_sql_query)
                    csv_writer = csv.writer(f, delimiter='|')
                    for record in cur.fetchall():
                        csv_writer.writerow(record)

            self._update_config_file('bi_directional_ds', output_path)
        except ps2.DatabaseError as e:
            raise ps2.DatabaseError(f"Database operation failed: {e}") from e
        except IOError as e:
            raise IOError(f"Failed to write to file at {output_path}: {e}") from e
        finally:
            if conn:
                conn.close()

    def create_attributes_ds(self, file_name: str) -> None:
        """
        Creates a Resnet attributes dataset file from SQL query execution, writes it to the specified
        file, and updates the configuration to include this new dataset.

        This method connects to a PostgreSQL database, executes a predefined SQL query to fetch
        attribute data, and writes the fetched records into a file at the specified location with
        a '.txt' extension. After successful creation, it updates the configuration file with the path
        to this newly created dataset.

        Args:
            file_name (str): The base name of the file where the dataset will be saved. The '.txt'
                             extension is appended if not already present.

        Raises:
            IOError: If there is an issue opening or writing to the output file.
            psycopg2.DatabaseError: If an error occurs connecting to the database or executing
                                    the SQL query.
        """
        output_path = os.path.join(self.base_path, file_name)
        if not output_path.endswith('.txt'):
            output_path += '.txt'
        conn = self.create_connection_to_postgresql_db()
        try:
            with conn.cursor() as cur:
                with open(output_path, 'w', encoding="utf-8") as f:
                    cur.execute(attributes_sql_query)
                    csv_writer = csv.writer(f, delimiter='|')
                    for record in cur.fetchall():
                        csv_writer.writerow(record)

            self._update_config_file('attributes_ds', output_path)
        except ps2.DatabaseError as e:
            raise ps2.DatabaseError(f"Database operation failed: {e}") from e
        except IOError as e:
            raise IOError(f"Failed to write to file at {output_path}: {e}") from e
        finally:
            if conn:
                conn.close()

    def create_nodes_ds(self, file_name: str) -> None:
        """
        Creates a nodes dataset file from SQL query execution, writes it to the specified
        file, and updates the configuration to include this new dataset.

        This method connects to a PostgreSQL database, executes a predefined SQL query to fetch
        nodes data, and writes the fetched records into a file at the specified location with
        a '.txt' extension. After successful data fetching and writing, it updates the configuration
        file with the path to this newly created dataset.

        Args:
            file_name (str): The base name of the file where the dataset will be saved. The '.txt'
                             extension is appended if not already present.

        Raises:
            IOError: If there is an issue opening or writing to the output file.
            psycopg2.DatabaseError: If an error occurs connecting to the database or executing
                                    the SQL query.
        """
        output_path = os.path.join(self.base_path, file_name)
        if not output_path.endswith('.txt'):
            output_path += '.txt'
        conn = self.create_connection_to_postgresql_db()
        try:
            with conn.cursor() as cur:
                with open(output_path, 'w', encoding="utf-8") as f:
                    cur.execute(node_sql_query)
                    csv_writer = csv.writer(f, delimiter='|')
                    for record in cur.fetchall():
                        csv_writer.writerow(record)

            self._update_config_file('nodes_ds', output_path)
        except ps2.DatabaseError as e:
            raise ps2.DatabaseError(f"Database operation failed: {e}") from e
        except IOError as e:
            raise IOError(f"Failed to write to file at {output_path}: {e}") from e
        finally:
            if conn:
                conn.close()


def main():
    """
    Entry point of the script that processes datasets based on command-line arguments.
    This function sets up a command-line parser to specify which dataset creation methods to
    call, and processes datasets accordingly based on the specified command-line arguments.
    It allows the user to select which method to use for dataset creation, define the base
    path for input/output files, and specify the filename for the dataset to be saved.

    Usage:
        python script_name.py --method create_directional_ds --file_name example.txt --base_path ./data/processed/

    Args:
        --method (str): One or more dataset methods to call. Possible choices include:
                        'create_directional_ds', 'create_bi_directional_ds',
                        'create_attributes_ds', 'create_nodes_ds'.
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
