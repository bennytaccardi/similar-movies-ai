from concurrent import futures
import grpc
import json
import services.service_pb2 as service_pb2
import services.service_pb2_grpc as service_pb2_grpc
from services.vectordb_service import load_persisted_index
import openai
from dotenv import load_dotenv
import os
from utils import build_prompt

load_dotenv()
api_key = os.getenv('OPENAI_API_KEY')
openai.api_key = api_key

class MyService(service_pb2_grpc.MyServiceServicer):
    def GetJson(self, request, context):
        search_string = request.search_string
        print(search_string)
        index = load_persisted_index()
        query_engine = index.as_query_engine()
        
        response = query_engine.query(build_prompt(search_string))

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
