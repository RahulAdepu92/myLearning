"""Module for database interfaces (aka DBI).
Most basic, common database functionality goes in here."""


import logging
import time
import pg8000
from ..redshift import get_redshift_credentials_as_tuple
from ..constants import SECRET_NAME_KEY
from ..glue import get_glue_logger
from typing import Iterable, List, Dict, Tuple
import random

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)


class AhubDb:
    """
    Wraps up database connectivity and querying in a Repository pattern-like class.
    The class provides query capabilities with functions that return records (query, get_one)
    and functions that execute queries (execute, _execute).

    For all the methods, the query paramaters are passed in "named" format,
    that is `:param_name`.
    """

    MAX_RETRIES: int = 5
    RETRY_DELAY_IN_SECONDS: float = 1.0

    def __init__(self, secret_name: str = None, tables_to_lock: Iterable[str] = None, **kwargs):
        if secret_name is None:
            if SECRET_NAME_KEY not in kwargs:
                raise KeyError(f"Unable to find {SECRET_NAME_KEY}")
            secret_name = kwargs[SECRET_NAME_KEY]

        self.__secret_name = secret_name
        self.__lock_tables = tables_to_lock
        self._conn = None

    # def commit(self):
    #    self._conn.commit()

    def iquery(self, query_string: str, **query_parameters) -> Tuple[List]:
        """
        Note: Use this when multiple rows are fetched (for ex: select * from table)

        Executes a query with named parameters and returns
        an enumeration where each entry is a row from the query
        mapped into a indexed list (each column is at the same
        position it was in the query).

        rows = db.iquery("select id, col2, col3 from footable where file_type= :type", type='INBOUND')
        for row in rows:
            print(row)
        > [1, "column two", "column three"]

        :return: a tuple where each entry is an indexed list representing
                 the individual rows, for example:
                 ([1, '1.col2', '1.col3'], [2, '2.col2', '2.col3'], ...)"""

        logger.debug("AhubDb.iquery query: %s, with params: %r.", query_string, query_parameters)
        with PgConn(self.__secret_name) as conn:
            rows = self.__run_with_retry(conn, query_string, **query_parameters)
            return rows

    def get_one(self, query_string: str, **query_parameters) -> Dict:
        """
        Note: Use this when one row is fetched (for ex: select * from table where id=:id, id =1)

        Retrieves the first record retrieved by the query.
        Convenience method for queries that only return one row,
        so that we don't have to call `next(db.query(...))` to get one record.

        row = db.get_one("select id, col2, col3 from footable where id = :id", id=5)
        print(row)
        > { 'id': 1, 'col2': "column two", 'col3': "column three" }
        print(row("id"))
        > 1

        row = db.get_one("select count(*)count from footable where id = :id", id=100)
        print(row)
        > { 'count': 0 }
        print(row("count"))
        > 0

        :return: iterable of a dictionary where the keys are the column names"""

        try:
            logger.debug("AhubDb.get_one query: %s, with params: %r.", query_string, query_parameters)

            with PgConn(self.__secret_name) as conn:
                rows = self.__run_with_retry(conn, query_string, **query_parameters)
                columns = [k[0] for k in conn.description]
                for row in rows:
                    return dict(zip(columns, row))
                return None
        except Exception:
            logger.exception("Error getting one %s with args: %r", query_string, query_parameters)
            raise

    def query(self, query_string: str, **query_parameters) -> Iterable[Dict]:
        """
        Note: Rarely used when fetched records are to be iterated

        Executes a query with named parameters and returns
        an enumeration where each entry is a row from the query
        mapped into a dictionary whose key is the column name.
        This is typically used for select statements. `execute`
        might be of interest for insert/update/delete statements.

        Example:
        rows = db.query("select id, col2, col3 from footable where id = :id", id=5)
        for row in rows:
            print(row)
        => { 'id': 1, 'col2': "column two", 'col3': "column three" }
        :return: iterable of a dictionary where the keys are the column names"""

        try:
            logger.debug("AhubDb.query %s, with params %r.", query_string, query_parameters)
            with PgConn(self.__secret_name) as conn:
                rows = self.__run_with_retry(conn, query_string, **query_parameters)
                columns = [k[0] for k in conn.description]
                for row in rows:
                    yield dict(zip(columns, row))
        except Exception:
            logger.exception(
                "Error executing query %s with args: %r", query_string, query_parameters
            )
            raise

    def execute(self, query: str, **query_parameters) -> int:
        """
        Note: 1. Use this when DML actions (insert,update,delete) are to be performed.
        2. tables_to_lock is mandatory argument to be passed. Else execute method fails with Nonetype object error.
        ex: AhubDb(secret_name=secret_name, tables_to_lock=[f"{SCHEMA_DW}.{TABLE_JOB}"]).execute(update_query)

        Executes a query with the specified parameters; commits at the end.

        db.execute("insert into foo (name) values (:name)", name="bar")
        :return: number of rows affected

        Note: _execute doesn't commit. You will need to do that yourself
        by calling commit() when appropriate
        """
        try:
            logger.debug("AhubDb.execute %s, with params %r.", query, query_parameters)

            with LockingPgConn(self.__secret_name, self.__lock_tables) as conn:
                rows_affected = self.__execute_with_retry(conn, query, **query_parameters)
                return rows_affected
        except Exception:
            self._conn.rollback()
            logger.exception("Error _executing %s with args: %r", query, query_parameters)
            raise

    def __run_with_retry(
        self, conn: pg8000.Connection, query_string, **query_parameters
    ) -> Iterable[Dict]:
        """Will attempt to re-run a query up to AhubDb.MAX_RETRIES if we receive a serialization error,
        each retry being delayed by a linearly increasing number of seconds, as defined by RETRY_DELAY_IN_SECONDS.
        :return: a tuple where each entry is an indexed list representing
                 the individual rows, for example:
                 ([1, '1.col2', '1.col3'], [2, '2.col2', '2.col3'], ...)"""
        attempt = 1
        while attempt <= AhubDb.MAX_RETRIES:
            try:
                rows = conn.run(query_string, **query_parameters)
                return rows
            except pg8000.ProgrammingError as e:
                # ProgrammingError sets the connection in an aborted mode so we need to rollback
                # 'C': '25P02', 'M': 'current transaction is aborted...
                conn.rollback()
                # the only repetitive ProgrammingError we seem to encounter
                # is the serialization exception, and that's recoverable with
                # the approach in here.
                # If we ever need to isolate it:
                # "'M': '1023'" in str(e)
                logger.warning(
                    "Error on run_retry %s/%s of $$ %s $$ with parameters %r because of error %r",
                    attempt,
                    AhubDb.MAX_RETRIES,
                    query_string,
                    query_parameters,
                    e,
                )
                attempt += 1
                if attempt > AhubDb.MAX_RETRIES:
                    raise
                time.sleep(AhubDb.RETRY_DELAY_IN_SECONDS * attempt)

    def __execute_with_retry(
        self, conn: pg8000.Connection, query_string, **query_parameters
    ) -> int:
        """Will attempt to re-execute a query up to AhubDb.MAX_RETRIES if we receive a serialization error,
        each retry being delayed by a linearly increasing number of seconds, as defined by RETRY_DELAY_IN_SECONDS.
        :return: number of rows affected"""
        attempt = 1
        while attempt <= AhubDb.MAX_RETRIES:
            try:
                cursor = conn.cursor()
                cursor.paramstyle = "named"
                cursor.execute(query_string, query_parameters)
                cursor.close()
                rows_affected = cursor.rowcount
                return rows_affected
            except pg8000.ProgrammingError as e:
                # ProgrammingError sets the connection in an aborted mode so we need to rollback
                # 'C': '25P02', 'M': 'current transaction is aborted...
                conn.rollback()
                logger.warning(
                    "Error on execute_retry %s/%s of $$ %s $$ with parameters %r because of error %s",
                    attempt,
                    AhubDb.MAX_RETRIES,
                    query_string,
                    query_parameters,
                    e,
                )
                attempt += 1
                if attempt > AhubDb.MAX_RETRIES:
                    raise
                time.sleep(AhubDb.RETRY_DELAY_IN_SECONDS * attempt)


class PgConn:
    def __init__(self, secret_name: str):
        logger.debug("Getting credentials for secret: %s", secret_name)
        (username, password, host, port, dbname) = get_redshift_credentials_as_tuple(secret_name)
        self.__username = username
        self.__password = password
        self.__host = host
        self.__port = port
        self.__dbname = dbname
        self.__conn = None

    def __enter__(self) -> pg8000.Connection:
        if self.__conn is None:
            self.__conn = pg8000.connect(
                user=self.__username,
                password=self.__password,
                host=self.__host,
                port=int(self.__port),
                database=self.__dbname,
            )
        return self.__conn

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            logger.warning("Encountered an error (%s, %s). Rolling back", exc_type, exc_value)
            self.__conn.rollback()
        else:
            self.__conn.commit()


class LockingPgConn(PgConn):
    def __init__(self, secret_name: str, tables_to_lock: List[str] = None):
        super().__init__(secret_name)
        self.__tables = tables_to_lock

    def __enter__(self) -> pg8000.Connection:
        """Changes as part of AHUB-731 (Locking causes deadlock)
        Parallel processing trying to lock the same table at the same time causing deadlock.
        Check has been made acquire the lock on the table only if it is not locked already by any other resource
        If the table is locked already, then we need to make the process to wait for some random delays
        and try to lock the table again if it is not loced already
        The reason for random delay is that all the parallel process should wait at random intervals ..
        not at the fixed interval which will make failures"""
        conn = super().__enter__()
        if self.__tables:
            list_of_table_name = ", ".join("'{0}'".format(w.split(".")[1]) for w in self.__tables)
            attempt = 1
            # Below line is to pick the random number between 3 and 7.
            # These values are picked after fine testing where value below 1 throws glue error while exporting multiple files
            # and greater than value 7 make lambda function to timeout.
            # So these are put as standard and shouldnt be disturbed until and unless if locking issue is raised again.
            retry_delay = round(random.uniform(3.0, 7.0), 1)
            # if the table is locked then retry will be made for 15 times
            while attempt <= 15:
                logger.info("Checking the Locks on table(s) %s", list_of_table_name)
                rows = conn.run(
                    f"select count(*) from svv_transactions t,svv_table_info i where t.relation=i.table_id and i.table in({list_of_table_name})"
                )
                if rows[0][0] > 0:
                    logger.info(
                        "The %s table(s) are locked by another resource already, will try after %s seconds",
                        list_of_table_name,
                        retry_delay * attempt,
                    )
                    # Below line is to make the process sleep for (retry_delay * attempt) seconds.
                    # if the random number is 4.5 then it will wait at 4.5(4.5*1),9(4.5*2),13.5(4.5*3),18(4.5*4),22.5(4.5*5),27(4.5*6)
                    # and so on till 15 iteration
                    time.sleep(retry_delay * attempt)
                    attempt += 1
                else:
                    all_tables = ", ".join(str(t) for t in self.__tables)
                    logger.info("Locking tables %s", all_tables)
                    conn.run(f"LOCK {all_tables};")
                    return conn
        return None
