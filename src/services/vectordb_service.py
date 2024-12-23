from llama_index.core import Document, VectorStoreIndex, StorageContext, load_index_from_storage
import csv
import os

PERSIST_DIR = "db_storage"

def load_and_persist_dataset(csv_path):
    documents = []

    # Read CSV as dictionary
    with open(csv_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            doc_content = " ".join([f"{key}: {value}" for key, value in row.items()])
            documents.append(Document(text=doc_content))

    index = VectorStoreIndex.from_documents(documents)
    index.storage_context.persist(persist_dir=PERSIST_DIR)  # Persist index to disk
    print(f"Index persisted to {PERSIST_DIR}")
    # query_engine = index.as_query_engine()

    # response = query_engine.query("What is The Shop Around the Corner")

    # print(response)

def load_persisted_index():
    # Load index from the persisted storage
    if not os.path.exists(PERSIST_DIR):
        raise FileNotFoundError(f"No persisted index found in {PERSIST_DIR}")

    storage_context = StorageContext.from_defaults(persist_dir=PERSIST_DIR)
    index = load_index_from_storage(storage_context)
    print("Index loaded successfully.")
    return index