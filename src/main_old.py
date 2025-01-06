from services.vectordb_service import load_and_persist_dataset, load_persisted_index
import openai
from dotenv import load_dotenv
import os
import sys
import time

load_dotenv()
api_key = os.getenv('OPENAI_API_KEY')
openai.api_key = api_key

def main():
    index = None
    args = sys.argv[1:]
    try:
        while True:
            
            if len(args) == 1 and args[0] == 'load':
                index = load_and_persist_dataset('movies_metadata.csv')
            if not index:
                index = load_persisted_index()
            
            query_engine = index.as_query_engine()

            response = query_engine.query("""
            I need a list of the 5 most similar films to 'Nosferatu', ranked by similarity. 
            Focus on factors such as:
            - Genre (e.g., sci-fi, drama, action)
            - Tone and Atmosphere (e.g., dark, lighthearted, suspenseful)
            - Themes (e.g., redemption, survival, family)
            - Plot Elements (e.g., time travel, heists, coming-of-age)
            - Character Dynamics (e.g., mentor-student, rivalries, ensemble casts)
            - Visual Style (if applicable, e.g., stylized, realistic, animated)
            - Director/Screenwriter Similarities (if relevant)
            
            Provide the list in descending order of similarity, with a short explanation for each recommendation highlighting the key overlapping elements with '<film-title>'.
            """)

            print(str(response))
            
            # Sleep for a while before the next iteration (e.g., 10 seconds)
            time.sleep(10)
            
    except KeyboardInterrupt:
        print("Task stopped by user. Exiting...")

if __name__ == "__main__":
    main()
