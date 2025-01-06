from concurrent import futures
import grpc
import json
import services.service_pb2 as service_pb2
import services.service_pb2_grpc as service_pb2_grpc
from services.vectordb_service import load_and_persist_dataset, load_persisted_index
import openai
from dotenv import load_dotenv
import os
import sys

load_dotenv()
api_key = os.getenv('OPENAI_API_KEY')
openai.api_key = api_key

class MyService(service_pb2_grpc.MyServiceServicer):
    def GetJson(self, request, context):
        args = sys.argv[1:]
        # index = None
        # if len(args) == 1 and args[0] == 'load':
        #     index = load_and_persist_dataset('movies_metadata.csv')
        # if not index:
        #     index = load_persisted_index()
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

        # JSON data to return
        data = {"message": str(response), "status": "success"}
        json_str = json.dumps(data)
        return service_pb2.JsonResponse(json_data=json_str)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_MyServiceServicer_to_server(MyService(), server)
    server.add_insecure_port("[::]:50051")
    print("Server is running on port 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
