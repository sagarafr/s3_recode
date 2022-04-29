from datetime import timezone, datetime

from cassandra import cluster
from flask import g

CLUSTER = ["scylla_db_1", "scylla_db_2", "scylla_db_3"]


def get_cluster() -> cluster.Cluster:
    global CLUSTER
    if 'cluster' not in g:
        g.cluster = cluster.Cluster(CLUSTER)

    return g.cluster


def get_session(session_name: str = None) -> cluster.Session:
    scylla_db = get_cluster()
    if "session" not in g:
        g.session = dict()

    if session_name not in g.session:
        g.session[session_name] = scylla_db.connect(session_name)

    return g.session[session_name]


def get_s3_metadata_session() -> cluster.Session:
    return get_session("s3_metadata")


def init_keyspace():
    session = get_session()
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS s3_metadata WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy','DC1' : 3};
        """
    )


def init_tables():
    session = get_s3_metadata_session()
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS bucket (
            bucket_name text,
            bucket_creation_date timestamp,
            access_key_id text,
            PRIMARY KEY (bucket_name, access_key_id)
        );
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS object (
            bucket_name text,
            object_name text,
            hdfs_path text,
            last_modified timestamp,
            PRIMARY KEY (bucket_name, object_name)
        );
        """
    )


def init_db():
    init_keyspace()
    init_tables()


def list_bucket(access_key_id: str) -> cluster.ResultSet:
    session = get_s3_metadata_session()
    return session.execute(
        f"""
        SELECT bucket_name, bucket_creation_date FROM bucket WHERE access_key_id='{access_key_id}';
        """
    )


def create_bucket(bucket_name: str, access_key_id: str):
    session = get_s3_metadata_session()
    now = datetime.now(timezone.utc).isoformat('T', 'seconds')
    session.execute(
        f"""
        INSERT INTO bucket (bucket_name, bucket_creation_date, access_key_id) VALUES ('{bucket_name}', '{now}', '{access_key_id}');
        """
    )


def delete_bucket(bucket_name: str, access_key_id: str):
    session = get_s3_metadata_session()
    session.execute(
        f"""
        DELETE FROM bucket WHERE bucket_name='{bucket_name}' AND access_key_id='{access_key_id}';
        """
    )


def is_bucket_already_exist(bucket_name: str):
    session = get_s3_metadata_session()
    rows = session.execute(
        f"""
        SELECT bucket_name from bucket where bucket_name='{bucket_name}';
        """
    )
    if not rows:
        return False
    return True


def create_object(bucket_name: str, object_name: str, hdfs_path: str):
    session = get_s3_metadata_session()
    now = datetime.now(timezone.utc).isoformat('T', 'seconds')

    session.execute(
        f"""
        INSERT INTO object (bucket_name, object_name, hdfs_path, last_modified) VALUES ('{bucket_name}', '{object_name}', '{hdfs_path}', '{now}');
        """
    )


def list_object(bucket_name: str):
    session = get_s3_metadata_session()
    return session.execute(
        f"""
        SELECT object_name, last_modified FROM object WHERE bucket_name='{bucket_name}';
        """
    )


def select_object(bucket_name: str, object_name: str):
    session = get_s3_metadata_session()
    return session.execute(
        f"""
        SELECT hdfs_path FROM object WHERE bucket_name='{bucket_name}' AND object_name='{object_name}';
        """
    )
