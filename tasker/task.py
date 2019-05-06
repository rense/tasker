import json
import time
import uuid

from tasker import constants
from tasker.utilities import redis_key_with_prefix as _k, redis_connection


class Task:
    # Handlers is filled dynamically by the "listens_to" decorator
    # (decorators.py)
    handlers = {}

    @staticmethod
    def create(task_type, **kwargs):
        c = redis_connection()
        c.lpush(
            _k(constants.QUEUE_REGULAR_TASKS),
            json.dumps({
                'key': str(uuid.uuid4()),
                'type': task_type,
                'data': kwargs
            }))

    @staticmethod
    def prioritize(task_type, **kwargs):
        c = redis_connection()
        c.rpush(
            _k(constants.QUEUE_REGULAR_TASKS),
            json.dumps({
                'key': str(uuid.uuid4()),
                'type': task_type,
                'data': kwargs
            }))

    @staticmethod
    def delay_unique(task_type, identifier=None, data=None, delay=10):
        c = redis_connection()

        key_delayed_task_data = _k(constants.DELAYED_TASK_DATA)
        key_queue_delayed_tasks = _k(constants.QUEUE_DELAYED_TASKS)

        data_key = "std%s" % identifier
        existing_data = c.hget(key_delayed_task_data, data_key)

        if existing_data:
            existing_data = json.loads(existing_data)
            existing_data.update(data)
            data = existing_data

        data = json.dumps(data)
        c.hset(key_delayed_task_data, data_key, data)

        item = json.dumps([data_key, task_type])

        # Remove old task (if any) in order to delay
        c.zrem(key_queue_delayed_tasks, item)

        # Add new task
        c.zadd(key_queue_delayed_tasks, item, time.time() + delay)

    @staticmethod
    def _get_next_delayed_task():
        return redis_connection().zrange(_k(constants.QUEUE_DELAYED_TASKS), 0, 0,
                                         withscores=True)

    @staticmethod
    def has_delayed_task_available():
        item = Task._get_next_delayed_task()

        if not item or item[0][1] > time.time():
            return False
        return True

    @staticmethod
    def queue_next_delayed_task(prioritize=False):
        rconn = redis_connection()
        item = Task._get_next_delayed_task()

        data_key, task_type = json.loads(item[0][0])

        # lock_identifier = acquire_lock(rconn, data_key)
        # if not lock_identifier:
        #     log.msg('\x1b[0;37;41m' + "Could not acquire lock.. Someone else "
        #                               "already working on this?" + '\x1b[0m')
        #     return None

        removed = rconn.zrem(_k(constants.QUEUE_DELAYED_TASKS), item[0][0])

        if removed:
            data = rconn.hget(_k(constants.DELAYED_TASK_DATA),
                              data_key)

            data = json.loads(data)

            if prioritize:
                tasks.Task.prioritize(task_type, **data)
            else:
                tasks.Task.create(task_type, **data)

            rconn.hdel(_k(tasks.DELAYED_TASK_DATA), data_key)

        # release_lock(rconn, data_key, lock_identifier)

    @staticmethod
    def fetch_task():
        rconn = redis_connection()
        queue_length = rconn.llen(_k(constants.QUEUE_REGULAR_TASKS))

        if queue_length > 0:
            m = rconn.rpop(_k(constants.QUEUE_REGULAR_TASKS))

            if not m:
                return False

            return json.loads(m)

        return False
