import grpc
import services.service_pb2 as service_pb2
import services.service_pb2_grpc as service_pb2_grpc

def run():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = service_pb2_grpc.MyServiceStub(channel)
        response = stub.GetJson(service_pb2.Empty())
        print(f"Received JSON: {response.json_data}")

if __name__ == "__main__":
    run()
