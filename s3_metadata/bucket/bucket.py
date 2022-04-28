from http import HTTPStatus

import dict2xml
from flask import blueprints, request, make_response

from s3_metadata.utils.db import create_bucket as db_create_bucket
from s3_metadata.utils.db import delete_bucket as db_delete_bucket
from s3_metadata.utils.db import is_bucket_already_exist as db_is_bucket_already_exist
from s3_metadata.utils.db import list_bucket as db_list_bucket
from s3_metadata.utils.utils import have_a_correct_name, get_access_key_id
from .all_buckets import Buckets, Bucket
from .error import bucket_already_exists, invalid_bucket_name

BP_BUCKET = blueprints.Blueprint('bucket', __name__)


@BP_BUCKET.route("/", methods=["GET"])
def list_bucket():
    access_key_id = get_access_key_id(request.headers)
    rows = db_list_bucket(access_key_id)

    buckets = {
        "ListAllMyBucketsResult":
            Buckets([
                Bucket(row.bucket_name, row.bucket_creation_date) for row in rows
            ]).to_dict()
    }
    response = make_response()
    response.data = dict2xml.dict2xml(buckets)

    return response


@BP_BUCKET.route("/<bucket>", methods=["PUT"])
def create_bucket(bucket: str):
    if not have_a_correct_name(bucket):
        return invalid_bucket_name(bucket)

    if db_is_bucket_already_exist(bucket):
        return bucket_already_exists(bucket)

    access_key_id = get_access_key_id(request.headers)
    db_create_bucket(bucket, access_key_id)
    response = make_response()
    response.headers["Location"] = bucket

    return response


@BP_BUCKET.route("/<bucket>", methods=["DELETE"])
def delete_bucket(bucket: str):
    access_key_id = get_access_key_id(request.headers)
    db_delete_bucket(bucket, access_key_id)
    response = make_response()
    response.status_code = HTTPStatus.NO_CONTENT
    return response
