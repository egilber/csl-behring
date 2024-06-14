## **Knowledge Graph Data Preparation Scripts**
This project consists of scripts that create and preprocess Resnet datasets using SQL queries and Metaflow for
pipeline management. The data is retrieved from a PostgreSQL database whose credentials are retrieved from a .env file. The 
scripts use the pydantic library to manage paths and database credentials.

### **Environment Setup**
This project uses a number of external Python libraries. You can recreate the required environment using the provided
environment.yml file. 

1.  Navigate to the project's root directory in your terminal and perform the following command:

    ```conda env create -f environment.yml```
    
2. **Environment Variables**: Set up a .env file to manage database credentials. The required variables include
 DB_NAME, DB_USER, DB_HOST_IP, and DB_PWD.

### **How to run the scripts**:
The pipelines can be run by executing the corresponding scripts while in the root of the directory:

1. To create the required datasets, run 'resnet_dataset_pipeline':

    ```python ./pipelines/resnet_dataset_pipeline run```
    
2. To preprocess the datasets, run the 'preprocess_resnet_pipeline':

    ```python ./pipelines/preprocess_resnet_pipeline run```
    
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