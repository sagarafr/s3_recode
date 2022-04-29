from dict2xml import dict2xml
from flask import Blueprint, make_response
from flask import current_app
from flask import request

from s3_metadata.bucket.error import no_such_bucket
from s3_metadata.bucket.error import no_such_key
from s3_metadata.utils.db import create_object as db_create_object, select_object as db_select_object
from s3_metadata.utils.db import is_bucket_already_exist as db_is_bucket_already_exist
from s3_metadata.utils.db import list_object as db_list_object
from .hadoop import create_directories
from .hadoop import delete_object as hadoop_delete_object
from .hadoop import get_object as hadoop_get_object
from .hadoop import send_object as hadoop_send_object

BP_OBJECT = Blueprint("object", __name__)


@BP_OBJECT.route("/<bucket>/<path:key>", methods=["PUT"])
def send_object(bucket: str, key: str):
    current_app.logger.debug(f"send object {bucket}")
    if not db_is_bucket_already_exist(bucket):
        return no_such_bucket(bucket)

    hdfs_path = f"{bucket}/{key}"

    db_create_object(bucket, key, hdfs_path)
    create_directories("/".join(hdfs_path.split("/")[1:-1]))
    hadoop_send_object(bucket, key, data=request.data)

    response = make_response()
    return response


@BP_OBJECT.route("/<bucket>", methods=["GET"])
def list_object(bucket: str):
    rows = db_list_object(bucket)
    current_app.logger.debug(f"rows {rows}")
    contents = [
        {
            "Key": row.object_name,
            "LastModified": row.last_modified,
            "StorageClass": "STANDARD"
        }
        for row in rows
    ]
    list_bucket_result = {
        "ListBucketResult": {
            "Name": bucket,
            "KeyCount": len(contents),
            "MaxKeys": 1000,
            "IsTruncated": False,
            "Contents": contents
        }

    }
    response = make_response()
    response.data = dict2xml(list_bucket_result)
    return response


@BP_OBJECT.route("/<bucket_name>/<path:key>", methods=["DELETE"])
def delete_object(bucket_name: str, key: str):
    rows = db_select_object(bucket_name, key)
    if not rows:
        return no_such_key(bucket_name, key)
    hdfs_path = [row.hdfs_path for row in rows]
    hadoop_delete_object(hdfs_path[0])
    response = make_response()
    return response


@BP_OBJECT.route("/<bucket_name>/<path:key>", methods=["GET"])
def get_object(bucket_name: str, key: str):
    rows = db_select_object(bucket_name, key)
    if not rows:
        return no_such_key(bucket_name, key)
    hdfs_path = [row.hdfs_path for row in rows]
    hadoop_response = hadoop_get_object(hdfs_path[0])
    response = make_response()
    response.data = hadoop_response.content
    return response
