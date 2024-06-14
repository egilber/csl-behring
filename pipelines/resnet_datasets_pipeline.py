import sys
import os

# Add the root of the project to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from metaflow import FlowSpec, step
    from src.scripts.resnet_datasets import CreateDatasets

    print("Imports successful.")
except Exception as e:
    print(f"Error during import: {e}")
    sys.exit(1)


class DataSetPipeline(FlowSpec):
    def __init__(self):
        super().__init__()
        self.cds = None

    @step
    def start(self):
        self.cds = CreateDatasets()
        self.next(self.create_directional_ds)

    @step
    def create_directional_ds(self):
        self.cds.create_directional_ds()
        self.next(self.create_bi_directional_ds)

    @step
    def create_bi_directional_ds(self):
        self.cds.create_bi_directional_ds()
        self.next(self.create_attributes_ds)

    @step
    def create_attributes_ds(self):
        self.cds.create_attributes_ds()
        self.next(self.create_nodes_ds)

    @step
    def create_nodes_ds(self):
        self.cds.create_nodes_ds()
        self.next(self.end)

    @step
    def end(self):
        print('Resnet datasets created successfully')


if __name__ == "__main__":
    DataSetPipeline()
