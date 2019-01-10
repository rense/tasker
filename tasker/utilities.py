import time
import uuid

import redis
from _mysql_exceptions import OperationalError
from django.db import connection as database_connection
from django.db.utils import OperationalError as DjangoOperationalError
from twisted.python import log
from django.conf import settings


def redis_key_with_prefix(key):
    return f"{settings.TASKER_KEY_PREFIX}_{key}"


singleton = False


def redis_connection():
    global singleton

    if not singleton:
        # log.msg('\x1b[0;37;41m' + "Creating redis connection" + '\x1b[0m')

        singleton = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_TASKER_DB
        )

    return singleton


def db_keepalive():
    # always have our DB-connection up and running

    try:
        database_connection.connection.ping()
    except (OperationalError, DjangoOperationalError, AttributeError):
        from django import db
        db.close_old_connections()
        db.connections.close_all()
        db.connection.close()
        log.msg('\x1b[0;37;41m' + "Closing old connections" + '\x1b[0m')


def acquire_lock(conn, lock_name, acquire_timeout=10):
    identifier = str(uuid.uuid4())

    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx('lock:' + lock_name, str.encode(identifier)):
            return identifier

        time.sleep(.001)

    return False


def release_lock(conn, lock_name, identifier):
    pipe = conn.pipeline(True)
    lock_name = 'lock:' + lock_name

    while True:
        try:
            # Check if the lock still exists

            pipe.watch(lock_name)
            if pipe.get(lock_name) == str.encode(identifier):
                # Releasing the lock
                pipe.multi()
                pipe.delete(lock_name)
                pipe.execute()
                # log.msg("Released lock (from release_lock)")
                return True
            else:
                log.msg("Couldn't release lock (not matching identifier)")

            pipe.unwatch()
            break

        except redis.exceptions.WatchError:
            pass  # Someone else did something with the lock; retry.

    return False  # We lost the lock
