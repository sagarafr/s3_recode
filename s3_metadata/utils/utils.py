import re

from flask import current_app


def get_access_key_id(headers: dict) -> str:
    authorization = headers.get('Authorization', '').split(',')
    credentials = [
        re.search(".*Credential=(.*)$", auth).group(1) for auth in authorization if re.match(r".*Credential.*", auth)
    ]
    credential = "" if len(credentials) == 0 else credentials[0]
    credential_split = credential.split("/")
    access_key_id = "" if len(credential_split) == 0 else credential_split[0]
    current_app.logger.debug(f"request {access_key_id}")
    return access_key_id


def have_a_correct_name(bucket_name):
    """
    Bucket names must be between 3 (min) and 63 (max) characters long.
    Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-).
    Bucket names must begin and end with a letter or number.
    Bucket names must not be formatted as an IP address (for example, 192.168.5.4).
    Bucket names must not start with the prefix xn--.
    Bucket names must not end with the suffix -s3alias. This suffix is reserved for access point alias names. For more information, see Using a bucket-style alias for your access point.
    Bucket names must be unique across all AWS accounts in all the AWS Regions within a partition. A partition is a grouping of Regions. AWS currently has three partitions: aws (Standard Regions), aws-cn (China Regions), and aws-us-gov (AWS GovCloud (US)).
    A bucket name cannot be used by another AWS account in the same partition until the bucket is deleted.
    Buckets used with Amazon S3 Transfer Acceleration can't have dots (.) in their names. For more information about Transfer Acceleration, see Configuring fast, secure file transfers using Amazon S3 Transfer Acceleration.

    :param bucket_name:
    :return: return False if bucket name is incorrect otherwise True
    """
    bucket_regex = "(?=^.{3,63}$)(?!^(\d+\.)+\d+$)(^(([a-z0-9]|[a-z0-9][a-z0-9\-]*[a-z0-9])\.)*([a-z0-9]|[a-z0-9][a-z0-9\-]*[a-z0-9])$)"
    pattern = re.compile(bucket_regex)
    if pattern.fullmatch(bucket_name) is None:
        return False
    return True




