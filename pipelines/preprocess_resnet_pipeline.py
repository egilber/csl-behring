import sys
import os

# Add the root of the project to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from metaflow import FlowSpec, step
    from src.scripts.preprocess_resnet import DataProcessor

    print("Imports successful.")
except Exception as e:
    print(f"Error during import: {e}")
    sys.exit(1)


class DataProcessingPipeline(FlowSpec):
    def __init__(self):
        super().__init__()
        self.dp = None

    @step
    def start(self):
        self.dp = DataProcessor()
        self.next(self.process_directional_rels)

    @step
    def process_directional_rels(self):
        self.dp.process_directional_rels()
        self.next(self.process_bi_directional_rels)

    @step
    def process_bi_directional_rels(self):
        self.dp.process_bi_directional_rels()
        self.next(self.process_attribute_rels)

    @step
    def process_attribute_rels(self):
        self.dp.process_attribute_rels()
        self.next(self.concat_relationship_files)

    @step
    def concat_relationship_files(self):
        self.dp.concat_relationship_files()
        self.next(self.process_node_file)

    @step
    def process_node_file(self):
        self.dp.process_node_file()
        self.next(self.end)

    @step
    def end(self):
        print('Resnet datasets successfully preprocessed')


if __name__ == "__main__":
    DataProcessingPipeline()

