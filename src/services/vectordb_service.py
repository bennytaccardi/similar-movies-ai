from llama_index.core import Document, VectorStoreIndex, StorageContext
import csv
import psycopg2
from sqlalchemy import make_url
from llama_index.core import StorageContext
from llama_index.core import VectorStoreIndex
from llama_index.core import Settings
from llama_index.vector_stores.postgres import PGVectorStore
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from datetime import datetime
from dotenv import load_dotenv
import os
import ast
import json

load_dotenv()

db_user = os.getenv('DB_USER')
db_pwd = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')
connection_string = f"postgresql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"
# connection_string = f"postgresql://postgres.zghxlbcgyvjfwmuhxiqr:provaProva!23@aws-0-us-west-1.pooler.supabase.com:6543/postgres?client_encoding=utf8"
db_name = "vectordb"

Settings.chunk_size = 4096
Settings.embed_model = HuggingFaceEmbedding(
    model_name="BAAI/bge-small-en-v1.5"
)

def create_combined_text(title, overview, release_date, genres, production_companies):
    return f"""Title: {title}
        Overview: {overview}
        Release Date: {release_date}
        Tags: {', '.join(genres)}
        Production Companies: {', '.join(production_companies)}"""

def safe_float(value):
   if not value or value.strip() == '':  # Handles None, '', whitespace
       return None
   try:
       return float(value)
   except (ValueError, TypeError):
       return None

def parse_json_list(value, key='name'):
   """Parse JSON list and extract values for given key"""
   if not value:
       return []
   try:
       items = ast.literal_eval(value) 
       return [item[key] for item in items if isinstance(item, dict)]
   except (ValueError, SyntaxError, TypeError, KeyError):
       return []
   
def load_and_persist_dataset(csv_path):
    # First, get existing document IDs from the database
    conn = psycopg2.connect(connection_string)
    conn.set_client_encoding('UTF8')
    cursor = conn.cursor()
    documents = []
    
    # Create tables if they don't exist
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS movie_metadata (
            id SERIAL PRIMARY KEY,
            doc_id TEXT,
            title TEXT NOT NULL,
            tags TEXT[],
            release_date DATE,
            popularity FLOAT,
            production_companies TEXT,
            vote_avg FLOAT
        )
    """)
    
    # Get existing doc_ids
    cursor.execute("SELECT doc_id FROM movie_metadata")
    existing_ids = set(row[0] for row in cursor.fetchall())
    
    # Read and process CSV
    with open(csv_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            doc_id = row.get('imdb_id')
            
            if doc_id in existing_ids:
                continue
                
            title = row.get('original_title', 'Untitled')
            overview = row.get('overview', '')
            vote_avg = safe_float(row.get('vote_average'))
            popularity = safe_float(row.get('popularity'))
            
            try:
                production_companies = parse_json_list(row.get('production_companies'))
                release_date = datetime.strptime(row.get('release_date', ''), '%Y-%m-%d').date() if row.get('release_date') else None
            except (ValueError, SyntaxError):
                tags = []
                production_companies = []
                release_date = None
            
            # Compute embedding
            combined_text = create_combined_text(title, overview, release_date, production_companies)
            # embedding = Settings.embed_model.get_text_embedding(combined_text)
            
            # Insert metadata
            cursor.execute(
                "INSERT INTO movie_metadata (doc_id, title, tags, vote_avg, release_date, popularity, production_companies) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (doc_id, title, tags, vote_avg, release_date, popularity, production_companies)
            )
            documents.append(
                Document(
                    text=overview,
                    doc_id=doc_id,
                    metadata={
                        "id": doc_id,
                        "title": title,
                        "tags": tags,
                        "vote_avg": vote_avg,
                        "realase_date": json.dumps(release_date, default=str),
                        "popularity": popularity,
                        "production_companies": production_companies
                    }
                )
            )
    
    if not documents:
        print("No new documents to add")
        conn.commit()
        conn.close()
        return
    
            
    conn.commit()
    conn.close()
    url = make_url(connection_string)
    vector_store = PGVectorStore.from_params(
        database=db_name,
        host=url.host,
        password=url.password,
        port=url.port,
        user=url.username,
        table_name="embeddings",
        embed_dim=384,
        hnsw_kwargs={
            "hnsw_m": 16,
            "hnsw_ef_construction": 64,
            "hnsw_ef_search": 40,
            "hnsw_dist_method": "vector_cosine_ops",
        },
    )

    # Create index with only new documents
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    index = VectorStoreIndex.from_documents(
        documents, 
        storage_context=storage_context, 
        show_progress=True,
    )
    print(f"Added new documents to the index")
    return index

def load_persisted_index():
    
    # Connect to the PostgreSQL database
    url = make_url(connection_string)

    # Initialize PGVectorStore
    vector_store = PGVectorStore.from_params(
        database=db_name,
        host=url.host,
        password=url.password,
        port=url.port,
        user=url.username,
        schema_name="public",
        table_name="embeddings",
        embed_dim=384,
        hnsw_kwargs={
            "hnsw_m": 16,
            "hnsw_ef_construction": 64,
            "hnsw_ef_search": 40,
        },
    )
    # Create storage context
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    # Load the index from the storage context
    index = VectorStoreIndex.from_vector_store(storage_context=storage_context, vector_store=vector_store)

    print("Index loaded successfully from PostgreSQL.")
    return index