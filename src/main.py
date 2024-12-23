from services.vectordb_service import load_and_persist_dataset, load_persisted_index
import openai
from dotenv import load_dotenv
import os
import sys

load_dotenv()
api_key = os.getenv('OPENAI_API_KEY')
openai.api_key = api_key

def main():
    args = sys.argv[1:]
    if len(args) == 1 and args[0] == 'load':
        load_and_persist_dataset('dataset_cut.csv')
    index = load_persisted_index()
    query_engine = index.as_query_engine()

    response = query_engine.query("What is The Shop Around the Corner")

    print(response)

if __name__ == "__main__":
    main()