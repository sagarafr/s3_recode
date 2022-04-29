import requests

HOST = "namenode:9870"


def create_directories(directories: str):
    global HOST
    return requests.put(f"http://{HOST}/webhdfs/v1/{directories}?op=MKDIRS")


def send_object(bucket_name, key, data):
    global HOST
    return requests.put(f"http://{HOST}/webhdfs/v1/{bucket_name}/{key}?op=CREATE&overwrite=true", data=data)


def delete_object(path: str):
    global HOST
    return requests.delete(f"http://{HOST}/webhdfs/v1/{path}?op=DELETE&recursive=false")


def get_object(path: str):
    global HOST
    return requests.get(f"http://{HOST}/webhdfs/v1/{path}?op=OPEN&noredirect=false")
