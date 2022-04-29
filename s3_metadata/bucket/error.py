from http import HTTPStatus

from dict2xml import dict2xml
from flask import make_response
from flask.wrappers import Response


def generate_error(code: str, message: str, resource: str, request_id: str, http_status: int) -> Response:
    error_msg = {
        "Error": {
            "Code": f"{code}",
            "Message": f"{message}",
            "Resource": f"{resource}",
            "RequestId": f"{request_id}"
        }
    }
    error = make_response()
    error.status_code = http_status
    error.data = dict2xml(error_msg)
    return error


def invalid_bucket_name(bucket_name: str):
    return generate_error("InvalidBucketName", "The specified bucket is not valid.",
                          f"{bucket_name}", "4442587FB7D0A2F9", HTTPStatus.BAD_REQUEST)


def bucket_already_exists(bucket_name: str):
    return generate_error("BucketAlreadyExists", "The requested bucket name is not available. The bucket namespace is "
                                                 "shared by all users of the system. Specify a different name and try again.",
                          f"{bucket_name}", "4442587FB7D0A2F9", HTTPStatus.CONFLICT)


def no_such_bucket(bucket_name: str):
    return generate_error("NoSuchBucket", "The specified bucket does not exist.", f"{bucket_name}", "4442587FB7D0A2F9",
                          HTTPStatus.NOT_FOUND)


def no_such_key(bucket_name: str, key: str):
    return generate_error("NoSuchKey", "The specified key does not exist.", f"{bucket_name}/{key}", "4442587FB7D0A2F9",
                          HTTPStatus.NOT_FOUND)
