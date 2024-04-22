import os
import json
import argparse
import pickle

import pandas as pd
import csv
from typing import Any, Optional, List, Tuple, List, NoReturn


def inOutkeys_to_lists(df):
    """
    Processes a DataFrame to extract and clean 'inOutkey' entries into separate lists of IDs.

    This function takes a DataFrame with an 'inOutkey' column, where each entry is expected to be a string
    representing a pair of IDs enclosed in brackets and separated by a comma (e.g., "[ID1,ID2]"). It
    cleans these strings by removing the brackets, splits them by the comma, and strips any surrounding
    whitespace, thereby creating two separate lists: one for the first IDs and one for the second IDs.

    Args:
        df (pd.DataFrame): A DataFrame containing the 'inOutkey' column with the described format.

    Returns:
        Tuple[List[str], List[str]]: A tuple of two lists; the first list contains the first IDs and
        the second list contains the second IDs extracted from the 'inOutkey' column.

    Examples:
        Given a DataFrame `df` with the following 'inOutkey' column:
            inOutkey
            "[123, 456]"
            "[789, 012]"
        Calling `inOutkeys_to_lists(df)` would return (['123', '789'], ['456', '012']).
    """
    first_ids = []
    second_ids = []
    df['inOutkey'] = df['inOutkey'].apply(lambda x: x.replace('[', '').replace(']', ''))

    for item in df['inOutkey'].tolist():
        x = item.split(',')
        first_ids.append(x[0].strip())
        second_ids.append(x[1].strip())

    return first_ids, second_ids


def convert_object_to_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts columns of the object dtype in a DataFrame to categorical dtype after stripping
    any leading or trailing whitespace.

    This function iterates over each column of type 'object' in the provided DataFrame,
    strips whitespace from the string entries, and converts the column to a 'category' dtype.
    This can often result in memory savings and speed improvements for operations involving
    categorical data, especially on large DataFrames.

    Args:
        df (pd.DataFrame): The DataFrame whose object-typed columns need to be converted.

    Returns:
        pd.DataFrame: The modified DataFrame with object columns converted to categorical.
    """
    df_obj = df.select_dtypes(['object'])

    for col in list(df_obj.columns):
        df[col] = df[col].apply(lambda x: str(x).strip())
        df[col] = df[col].astype('category')
    return df


def ensure_extension(file_name: str, extension: str) -> str:
    """
    Ensures that the output path has the specified file extension. If the output path does not
    already end with the given extension, the extension is appended to the output path.

    This function is useful for file handling operations where a specific file extension is
    required, ensuring consistency in file naming conventions and preventing errors related
    to file type mismatches.

    Args:
        file_name (str): The initial file name which may or may not include the desired extension.
        extension (str): The desired file extension to ensure is at the end of the output path.
                         The extension should include the dot ('.') if it is required, e.g., '.txt'.

    Returns:
        str: The output path guaranteed to end with the specified extension.
    """
    if not file_name.endswith(extension):
        file_name += extension
    return file_name


class DataProcessor:
    def __init__(self, base_path: str):
        """
        Initializes the DataProcessor class for managing and processing various datasets related to a specific project or analysis.

        The constructor sets up paths for managing different datasets including directional, bidirectional, attributes, and nodes datasets.
        These dataset paths are initialized to `None` and are expected to be defined later as the application progresses.

        Args:
            base_path (str): The root directory where datasets are stored or will be stored.

        Attributes:
            directional_ds (Optional[str]): Path to the directional dataset, initially None.
            bi_directional_ds (Optional[str]): Path to the bidirectional dataset, initially None.
            attributes_ds (Optional[str]): Path to the attributes dataset, initially None.
            nodes_ds (Optional[str]): Path to the nodes dataset, initially None.
            procd_biDirect_rels (Optional[str]): Path to the processed bidirectional relationships dataset, initially None.
            procd_direct_rels (Optional[str]): Path to the processed directional relationships dataset, initially None.
            procd_attrib_rels (Optional[str]): Path to the processed attributes relationships dataset, initially None.
        """
        self.base_path = base_path
        self.directional_ds = None
        self.bi_directional_ds = None
        self.attributes_ds = None
        self.nodes_ds = None
        self.procd_biDirect_rels = None
        self.procd_direct_rels = None
        self.procd_attrib_rels = None

    def _update_config_file(self, key: str, value: str) -> None:
        """
        Updates the configuration file `file_paths.json` located at `self.base_path` with the provided key-value pair.
        If the configuration file does not exist, it creates a new one. This method handles the reading, updating,
        and writing of the JSON configuration file.

        Args:
            key (str): The key to be added or updated in the configuration file.
            value (str): The value to be assigned to the key in the configuration file.

        Returns:
            None: This method does not return a value but modifies the configuration file directly.

        Raises:
            IOError: If there is an error in opening, reading, or writing to the file.
            json.JSONDecodeError: If there is an error decoding the JSON data from the file during reading.
        """
        config_file = os.path.join(self.base_path, 'file_paths.json')
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                file_paths = json.load(f)
        else:
            file_paths = {}
        file_paths[key] = value
        with open(config_file, 'w') as f:
            json.dump(file_paths, f)

    def _load_dataset_file_paths(self) -> None:
        """
        Load dataset file paths from the configuration file `file_paths.json` located
        at `self.base_path`.

        This method updates the internal state of the object by setting various dataset path
        attributes according to the contents found in the `file_paths.json` configuration file.

        Returns:
            None: This method does not return a value but updates the instance variables of the class.

        Raises:
            IOError: If there is an error in opening/reading the file.
            json.JSONDecodeError: If there is an error decoding the JSON data from the file.
        """
        config_file = os.path.join(self.base_path, 'file_paths.json')
        with open(config_file, 'r') as f:
            file_paths = json.load(f)
        self.directional_ds = file_paths.get('directional_ds')
        self.bi_directional_ds = file_paths.get('bi_directional_ds')
        self.attributes_ds = file_paths.get('attributes_ds')
        self.nodes_ds = file_paths.get('nodes_ds')
        self.procd_biDirect_rels = file_paths.get('procd_biDirect_rels')
        self.procd_direct_rels = file_paths.get('procd_direct_rels')
        self.procd_attrib_rels = file_paths.get('procd_attrib_rels')

    def create_header_file(self, cols: List[str], file_name: str, relationship: bool = True) -> None:
        """
        Creates a CSV header file with the specified columns and updates the configuration file
        with the path to this header file. The file can represent either a relationship or node
        header based on the 'relationship' parameter.

        Args:
            cols (List[str]): A list of column names to be included in the header file.
            file_name (str): The name of the file to save the header to. The '.txt' extension is
                             appended if not already present.
            relationship (bool): Determines whether the header file is for relationships or nodes.
                                 If True, it is treated as a relationship header; otherwise, as a nodes header.

        This function first ensures that the output file has the correct '.txt' extension, then
        creates a DataFrame using the provided columns and saves this DataFrame to a file
        at the specified path. Finally, it updates the configuration file to store the path
        to this new header file under the appropriate category (relationship or nodes).

        Returns:
            None: This function does not return any value.
        """
        file_name = ensure_extension(file_name, '.txt')
        output_path = os.path.join(self.base_path, file_name)

        df_headers = pd.DataFrame(columns=cols)
        df_headers.to_csv(output_path, sep='|', index=False)
        if relationship:
            self._update_config_file('relations_header', str(output_path))
        else:
            self._update_config_file('nodes_header', str(output_path))

    def process_bi_directional_rels(self, file_name: str) -> None:
        """
        Processes bi-directional relationship data from a CSV file, transforms it, and serializes it to a binary file.

        This method reads bi-directional relationship data from a specified CSV file, transforms the data by
        splitting 'inOutkey' into separate 'START_ID' and 'END_ID', dropping unnecessary columns, and converting
        relevant object columns to categorical data types. The processed DataFrame is then serialized to a binary
        file with a '.pkl' extension for efficient storage.

        Args:
            file_name (str): The name of the file where the processed data will be saved. The file name
                             will have a '.pkl' extension ensured by the `ensure_extension` function.

        Returns:
            None: This function does not return any value but updates the internal state by writing the processed
                  data to a file and updating the configuration file to record the path of the processed data.

        Raises:
            IOError: If there is an issue with file operations.
            pickle.PicklingError: If there is an error during the serialization process.
        """
        file_name = ensure_extension(file_name, '.pkl')
        output_path = os.path.join(self.base_path, file_name)

        df = pd.read_csv(self.bi_directional_ds, sep='|', header=None, encoding='utf-8')
        df.columns = ['msrc_id', ':START_ID', 'inOutkey', 'type:TYPE', 'relationship', 'effect', 'mechanism',
                      'ref_count:int', ':END_ID', 'id2', 'biomarkertype', 'celllinename', 'celltype',
                      'changetype', 'organ', 'organism', 'quantitativetype', 'tissue']
        first_ids, second_ids = inOutkeys_to_lists(df)
        df.drop(columns=['inOutkey', 'id2', 'relationship'], inplace=True)
        df[':START_ID'] = first_ids
        df[':END_ID'] = second_ids
        df[':START_ID'] = df[':START_ID'].astype('int64')
        df[':END_ID'] = df[':END_ID'].astype('int64')
        df['ref_count:int'] = df['ref_count:int'].astype('int16')
        df = convert_object_to_category(df)
        with open(output_path, 'wb') as file:
            pickle.dump(df, file)
        self._update_config_file('procd_biDirect_rels', str(output_path))

    def process_directional_rels(self, file_name: str) -> None:
        """
        Processes directional relationship data from a file, transforms it, and serializes it to a binary file.

        This method reads directional relationship data from a specified file, cleans and
        converts certain fields, then serializes the DataFrame to a binary file using pickle
        for efficient storage. This file is saved with a '.pkl' extension to indicate its format.

        Args:
            file_name (str): The name of the file where the processed data will be saved. The file name
                             will have a '.pkl' extension ensured by the `ensure_extension` function.

        Returns:
            None: This function does not return any value but updates the internal state by writing the processed
                  data to a file and updating the configuration file to record the path of the processed data.

        Raises:
            IOError: If there is an issue with file operations.
            pickle.PicklingError: If there is an error during the serialization process.
        """
        file_name = ensure_extension(file_name, '.pkl')
        output_path = os.path.join(self.base_path, file_name)

        df = pd.read_csv(self.directional_ds, sep='|', header=None, encoding='utf-8')
        df.columns = ['msrc_id', ':START_ID', 'type:TYPE', 'effect', 'mechanism', 'ref_count:int', ':END_ID',
                      'id2', 'biomarkertype', 'celllinename', 'celltype', 'changetype', 'organ', 'organism',
                      'quantitativetype', 'tissue', 'nct_id', 'phase']
        df = df.drop(columns=['id2'])
        df['phase'] = df['phase'].fillna('None')
        df = convert_object_to_category(df)
        df['ref_count:int'] = df['ref_count:int'].astype('int16')
        with open(output_path, 'wb') as file:
            pickle.dump(df, file)
        self._update_config_file('procd_direct_rels', str(output_path))

    def process_attribute_rels(self, file_name: str) -> None:
        """
        Processes attribute relationship data from a CSV file, transforms it, and serializes it to a binary file.

        This method reads attribute relationship data from a specified file, cleans and
        converts specific fields, then serializes the DataFrame to a binary file using pickle
        for efficient storage. The data is filtered to exclude rows where 'type:TYPE' is 'None',
        and unnecessary columns are removed. The final DataFrame is optimized by converting
        relevant fields to appropriate data types for better storage efficiency.

        Args:
            file_name (str): The name of the file where the processed data will be saved. The file name
                             will have a '.pkl' extension ensured by the `ensure_extension` function.

        Returns:
            None: This function does not return any value but updates the internal state by writing the processed
                  data to a file and updating the configuration file to record the path of the processed data.

        Raises:
            IOError: If there is an issue with file operations.
            pickle.PicklingError: If there is an error during the serialization process.
        """
        file_name = ensure_extension(file_name, '.pkl')
        output_path = os.path.join(self.base_path, file_name)

        df = pd.read_csv(self.attributes_ds, sep='|', header=None, encoding='utf-8', low_memory=False)
        df = df.fillna('None')
        df.columns = ['msrc_id', ':START_ID', 'id2', 'type:TYPE', ':END_ID']
        df = df[df['type:TYPE'] != 'None']
        df.reset_index(drop=True, inplace=True)
        df = df.drop(columns=['id2'])
        for col in list(df.columns):
            df[col] = df[col].apply(lambda x: str(x).strip())
        df[':START_ID'] = pd.to_numeric(df[':START_ID'], errors='coerce').astype('int64')
        df[':END_ID'] = pd.to_numeric(df[':END_ID'], errors='coerce').astype('int64')

        df['type:TYPE'] = df['type:TYPE'].astype('category')
        with open(output_path, 'wb') as file:
            pickle.dump(df, file)
            self._update_config_file('procd_attrib_rels', str(output_path))

    def concat_relationship_files(self, file_name: str) -> None:
        """
        Concatenates various relationship datasets into a single file and writes the result to a file.

        This method loads multiple datasets, specifically directional, bidirectional, and attribute
        datasets, concatenates them into a single DataFrame, processes and normalizes the data, then
        writes the consolidated DataFrame to a CSV file. Additionally, it creates a header file for
        the relationships.

        Args:
            file_name (str): The name of the output file where the concatenated data will be saved.
                             The '.txt' extension is ensured by the `ensure_extension` function.

        Returns:
            None: This function does not return any value but performs file I/O and updates internal
                  configuration by saving processed data and updating related metadata.

        Raises:
            IOError: If there is an issue with file operations.
            pd.errors: If there is an error during DataFrame operations like concatenation or file writing.
        """
        file_name = ensure_extension(file_name, '.txt')
        output_path = os.path.join(self.base_path, file_name)

        df_directional = pd.read_pickle(self.procd_direct_rels)
        df_biDirectional = pd.read_pickle(self.procd_biDirect_rels)
        df_att = pd.read_pickle(self.procd_attrib_rels)

        df_concat = pd.concat([df_directional, df_biDirectional, df_att])
        df_concat['ref_count:int'].fillna(0, inplace=True)
        # Convert all categorical columns to object type
        for col in df_concat.select_dtypes(include=['category']).columns:
            df_concat[col] = df_concat[col].astype('object')
        df_concat.fillna('None', inplace=True)
        df_concat = df_concat.replace('nan', 'None')
        df_concat = df_concat.replace('None', '_')

        df_concat['ref_count:int'] = df_concat['ref_count:int'].astype('int16')
        df_concat['msrc_id'] = df_concat['msrc_id'].astype('int64')
        df_concat = convert_object_to_category(df_concat.drop_duplicates().reset_index(drop=True))

        df_concat['type:TYPE'] = df_concat['type:TYPE'].apply(lambda x: x.upper())

        cols = list(df_directional.columns)
        df_concat[cols].to_csv(output_path, sep='|', index=False, header=True)
        self._update_config_file('concat_relations_file', str(output_path))

        self.create_header_file(cols, file_name='relationships_header', relationship=True)

    def process_node_file(self, file_name: str) -> None:
        """
        Processes node data from a CSV file, modifies and enriches it, and then saves it as a CSV file.

        This method reads node data from a CSV file specified by `nodes_ds`, formats, and cleans the data by adjusting
        data types and modifying string content to ensure consistency and proper formatting. Specifically, it handles
        complex data entries by reformatting them and ensuring all identifiers and labels are in the correct format.
        The processed data is then saved in a new CSV file at a location determined by the `file_name` argument.

        Args:
            file_name (str): The name of the output file where the processed node data will be saved. The '.txt' extension
                             is ensured by the `ensure_extension` function.

        Returns:
            None: This function does not return any value but updates the internal state by writing the processed
                  data to a file and updating the configuration to include this new node header file.

        Raises:
            IOError: If there is an issue with file operations such as reading from or writing to the disk.
            pd.errors.ParserError: If there is an error during the parsing of the CSV data.
        """
        file_name = ensure_extension(file_name, '.txt')
        output_path = os.path.join(self.base_path, file_name)

        df = pd.read_csv(self.nodes_ds, sep='|', header=None, encoding='utf-8', low_memory=False)
        df.columns = [':ID', 'name', ':LABEL']
        df = df.applymap(lambda x: str(x).strip())
        df[':LABEL'] = df[':LABEL'].apply(lambda x: x.upper())
        df[':ID'] = df[':ID'].astype('int64')
        df['name'] = df['name'].replace([';;', ';'], ':', regex=True)

        df.to_csv(output_path, sep='|', index=False, header=True)
        self._update_config_file('procd_node_file', str(output_path))

        self.create_header_file(list(df.columns), file_name='nodes_header', relationship=False)


def main() -> None:
    """
    Entry point of the script that processes datasets based on command-line arguments.

    The function sets up a command-line parser to specify which dataset processing methods to call,
    and processes datasets accordingly based on the specified command-line arguments. It allows the
    user to select which method to use for dataset processing, define the base path for input/output files,
    and specify the filename for the dataset to be saved.

    The available methods include processing bi-directional relationships, directional relationships,
    attribute relationships, concatenating relationship files, and processing node files.

    Usage:
        python script_name.py --method process_directional_rels --base_path './data/processed/' --file_name 'output.pkl'

    Args:
        --method (str): One or more dataset processing methods to call. Choices include
                        'process_bi_directional_rels', 'process_directional_rels',
                        'process_attribute_rels', 'concat_relationship_files', 'process_node_file'.
        --base_path (str): Base directory path where datasets are read from and saved to.
                           Defaults to './data/processed/'.
        --file_name (str): Name of the file to save the dataset to. This argument is required.

    Raises:
        Exception: Raises an exception if an invalid method is specified.
    """
    parser = argparse.ArgumentParser(description='Choose the methods to call.')
    parser.add_argument('--method', type=str, nargs='+', choices=['process_bi_directional_rels',
                                                                  'process_directional_rels',
                                                                  'process_attribute_rels',
                                                                  'concat_relationship_files',
                                                                  'process_node_file'])

    parser.add_argument('--base_path', type=str, default='./data/processed/', help='The base path for the input and '
                                                                                   'output files.')
    parser.add_argument('--file_name', type=str, required=True,
                        help='The name of the parquet file to save the dataset to.')
    args = parser.parse_args()

    dp = DataProcessor(base_path=args.base_path)
    dp._load_dataset_file_paths()  # Load the dataset file paths

    for method in args.method:
        if method == 'process_bi_directional_rels':
            dp.process_bi_directional_rels(args.file_name)
        elif method == 'process_directional_rels':
            dp.process_directional_rels(args.file_name)
        elif method == 'process_attribute_rels':
            dp.process_attribute_rels(args.file_name)
        elif method == 'concat_relationship_files':
            dp._load_dataset_file_paths()  # Load the dataset file paths
            dp.concat_relationship_files(args.file_name)
        elif method == 'process_node_file':
            dp.process_node_file(args.file_name)
        else:
            print("Invalid method choice. Use --help for more information.")


if __name__ == "__main__":
    main()
