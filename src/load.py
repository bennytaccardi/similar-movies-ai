from services.vectordb_service import load_and_persist_dataset
import sys 

def load ():
    dataset_path = './dataset/ds_copy.csv'
    args = sys.argv[1:]
    
    if len(args) == 1:
        dataset_path = args[0]
    load_and_persist_dataset(dataset_path)
    
if __name__ == "__main__":
    load()