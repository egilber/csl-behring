## **Knowledge Graph Data Preparation Scripts**

This repository contains two Python scripts designed to automate the creation and preprocessing of datasets for
uploading into a Neo4j database to build a knowledge graph. The first script, create_datasets.py, handles the
extraction and initial formatting of data from a PostgreSQL database. The second script, preprocess_datasets.py,
processes the data further to ensure compatibility and optimized structure for Neo4j.

### **Environment Setup**

1. **Python Installation**: Ensure Python 3.x is installed on your system.
2. **Dependency Installation**: Install required Python packages:

    ```pip install psycopg2 pandas numpy python-dotenv```
3. **Environment Variables**: Set up the necessary environment variables or use a .env file to manage database credentials 
and other configuration settings. The required variables include DB_NAME, DB_USER, DB_HOST_IP, and DB_PWD.

### **Scripts Overview**
#### **1. 'create_datasets.py'**

This script interacts with a PostgreSQL database to fetch data based on predefined SQL queries and saves the output 
in CSV format. It handles four main types of datasets:

* Directional
* Bi-directional
* Attributes
* Nodes

Each dataset is saved with a .txt extension and is intended for further processing.

**Running the Script**

To run create_datasets.py, use the following command, specifying the method and the output filename:

   ```python ./src/scripts/create_datasets.py --method create_directional_ds --file_name directional.txt --base_path ./data/processed/```

Available methods:

* create_directional_ds
* create_bi_directional_ds
* create_attributes_ds
* create_nodes_ds

#### **2. 'preprocess_datasets.py'**

After the datasets are created, preprocess_datasets.py takes these datasets and processes them to align with the 
requirements of Neo4j. This includes type conversions, splitting keys, and preparing headers for Neo4j import tools.

**Running the Script**

To run preprocess_datasets.py, specify the processing method and the input file:

   ```python ./src/scripts/preprocess_datasets.py --method process_bi_directional_rels --file_name bi_directional.pkl --base_path ./data/processed/```

Available methods:

* process_bi_directional_rels
* process_directional_rels
* process_attribute_rels
* concat_relationship_files
* process_node_file

### **Configuration**
Both scripts use a configuration file (file_paths.json) to manage dataset paths, which is automatically 
updated and read by the scripts as needed.

### **Loading Data Into Neo4j**

To load data into Neo4j, click on 'Add' in the project pane of the Neo4j application. Name the new project, click on 
the '...' by the 'Open' button for the project, and choose 'Terminal'. Take note of the dbms number at the cursor. 
Copy the nodes.txt, nodes_header.txt, relations.txt, and relations_header.txt files prepared above. Drop them in the 
import directory found here:

C:\Users\[user_name]\.Neo4jDesktop\relate-data\dbmss\dbms-##(number at cursor)\import

At the Neo4j terminal, change to the bin directory (type 'cd bin') and then paste the following at the cursor: 

   ```neo4j-admin import --delimiter="|" --nodes=import/nodes_header.txt, import/nodes.txt --relationships=import/relations_header.txt, import/relations.txt --skip-bad-relationships=true```

Go back to the project pane, press 'Start'. The active database will now be shown in a pane at the top. Press open to 
use in the Neo4j browser.


