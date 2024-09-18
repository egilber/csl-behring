import psycopg2 as ps2
import os
import csv
from dotenv import load_dotenv
load_dotenv()

database_name = 'resnet18'

from configs.database import SQLDBCreds
from configs.paths import DataPaths

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
WHERE (control.id = control.attributes and inkey[1] is not null and outkey[1] is not null)"

node_sql_query = f"select id, name, nodetype from {database_name}.node where id is not null and name is not null and nodetype \
 is not null"


class CreateDatasets:
    def __init__(self) -> None:
        """
        Initialize the CreateDatasets object, setting up paths.
        """
        self.paths = DataPaths()

    def get_sqldb_creds(self) -> SQLDBCreds:
        db_creds = SQLDBCreds(
            db_name=os.getenv('DB_NAME'),
            db_user=os.getenv('DB_USER'),
            db_host=os.getenv('DB_HOST_IP'),
            db_pwd=os.getenv('DB_PWD')
        )
        return db_creds

    def execute_sql_query(self, sql_query: str, output_path) -> None:
        """
        Execute the given SQL query and save the results to a file.

        Args:
            sql_query (str): The SQL query to execute.
            output_path (str): The path to the output file.
        """
        creds = self.get_sqldb_creds()
        conn = ps2.connect(
            dbname=creds.db_name,
            user=creds.db_user,
            password=creds.db_pwd,
            host=creds.db_host
        )
        try:
            with conn.cursor() as cur:
                with open(output_path, 'w', encoding="utf-8") as f:
                    cur.execute(sql_query)
                    csv_writer = csv.writer(f, delimiter='|')
                    for record in cur.fetchall():
                        csv_writer.writerow(record)
        except ps2.DatabaseError as e:
            raise ps2.DatabaseError(f"Database operation failed: {e}") from e
        except IOError as e:
            raise IOError(f"Failed to write to file at {output_path}: {e}") from e
        finally:
            if conn:
                conn.close()

    def create_directional_ds(self) -> None:
        """
        Create a dataset for directional relationships and save it to a file.
        """
        output_path = self.paths.get_full_path(self.paths.directional_rels)
        self.execute_sql_query(directional_sql_query, output_path)

    def create_bi_directional_ds(self) -> None:
        """
        Create a dataset for bidirectional relationships and save it to a file.
        """
        output_path = self.paths.get_full_path(self.paths.bidirectional_rels)
        self.execute_sql_query(bi_directional_sql_query, output_path)

    def create_attributes_ds(self) -> None:
        """
        Create a dataset for attribute relationships and save it to a file.
        """
        output_path = self.paths.get_full_path(self.paths.attribute_rels)
        self.execute_sql_query(attributes_sql_query, output_path)

    def create_nodes_ds(self) -> None:
        """
        Create a dataset for nodes and save it to a file.
        """
        output_path = self.paths.get_full_path(self.paths.nodes_raw)
        self.execute_sql_query(node_sql_query, output_path)
