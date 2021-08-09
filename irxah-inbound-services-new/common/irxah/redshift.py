import json
import logging
import os
from typing import Tuple

import boto3
from botocore.exceptions import ClientError

from .glue import get_glue_logger

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)

session = boto3.session.Session()
region_name = session.region_name


def get_redshift_credentials_as_json(secret_name: str, region_name: str) -> str:
    """Returns the redshift credentials as a JSON string.
    :param secret_name: Optional parameter. Specify the secret_name to establish Redshift connection.
    :param region_name: the region in which the credentials are stored (default us-east-1)
    :return: The JSON will contain the following keys:
    username, password, host, port, dbname
    """

    secret = ""
    # secret_name = f"irx-ahub-{environment}-redshift-cluster"

    # Create a Secrets Manager client
    logging.debug("Getting Secret %s", secret_name)
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as error:
        if error.response["Error"]["Code"] == "DecryptionFailureException":
            raise error
        if error.response["Error"]["Code"] == "InternalServiceErrorException":
            raise error
        if error.response["Error"]["Code"] == "InvalidParameterException":
            raise error
        if error.response["Error"]["Code"] == "InvalidRequestException":
            raise error
        if error.response["Error"]["Code"] == "ResourceNotFoundException":
            raise error
    else:
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]

    return secret


def get_redshift_credentials_as_tuple(secret_name: str) -> Tuple[str, str, str, str, str]:
    """Returns the redshift credentials as tuple.
    :param secret_name: Optional parameter. Specify the secret_name to establish Redshift connection.
    :return: The tuple will contain the following keys:
    username, password, host, port, dbname
    """
    secret_name = secret_name if secret_name is not None else os.environ.get("SECRET_NAME", "dv")
    secret_json = get_redshift_credentials_as_json(secret_name, region_name)
    secret = json.loads(secret_json)
    return (
        secret.get("username"),
        secret.get("password"),
        secret.get("host"),
        secret.get("port"),
        secret.get("dbname"),
    )


# As part of 05/14 release, we have decommissioned usage of pg module and its associated connections.
# https://jira.ingenio-rx.com/browse/AHUB-621
"""
def get_connection(secret_name: str = None):
    """"""Returns a redshift connection
    Keyword Arguments:
        secret_name {str} -- [description] (default: {None})
    Returns:
        [type] -- [description]
    """"""
    secret_name = secret_name if secret_name is not None else os.environ.get("SECRET_NAME", "dv")
    secret_json = get_redshift_credentials_as_json(secret_name, region_name)
    secret = json.loads(secret_json)
    redshift_user = secret.get("username")
    redshift_password = secret.get("password")
    redshift_host = secret.get("host")
    redshift_port = secret.get("port")
    redshift_database = secret.get("dbname")
    conn = create_redshift_connection(
        redshift_host, redshift_port, redshift_database, redshift_user, redshift_password
    )
    return conn


def create_redshift_connection(host: str, port: str, db_name: str, user: str, password: str):
    """"""
    Function to establish a connection with Redshift using psycopg2
    :param host: Host (IP) of the Redshift cluster
    :param port: Port of the Redshift database
    :param db_name: Name of the Redshift database
    :param user: Name of the Redshift master user
    :param password: Password for the Redshift master password
    :return: Returns a Redshift connection object
    """"""

    conn_string = "host={} port={} dbname={} user={} password={}".format(
        host,
        port,
        db_name,
        user,
        password,
    )
    conn = pg.connect(dbname=conn_string)
    return conn


def execute_query(conn, query: str, query_args: Optional[Iterable] = None):
    """"""Executes a query against a connection.
    :param conn: a conn opened with get_connection
    :param  query: a sql query. can be parametrized using old-style pg ($1, $2)
    :param query_args: a positional iterable (list, tuple) matching the parameters in query
    :return: a query object if select, or the number of affected rows if insert/update/delete

    :Example:
    >>> result = execute_query(conn, "select top 5 from schema.table")
    >>> result.getresult() # rows as list of tuples
    >>> result = execute_query(conn, "select * from schema.table where key=$1", (5, ))
    >>> result.dictresult() # rows as list of dictionaries
    >>> execute_query(conn, "insert into schema.table (col1, col2) values ($1, $2)", ['a', 2])
    1
    """"""
    if query_args is not None:
        try:
            return conn.query(query, query_args)
        except pg.ProgrammingError:
            logging.exception("Error executing query: %s with parameters: %r", query, query_args)
            return None
    else:
        try:
            return conn.query(query)
        except pg.ProgrammingError:
            logging.exception("Error executing query: %s", query)
            return None    
"""
