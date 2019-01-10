import json
import signal
import time
import uuid

from django.conf import settings
from twisted.application import service
from twisted.internet import reactor
from twisted.internet.reactor import callLater as call_later
from twisted.internet.threads import deferToThread as defer_to_thread
from twisted.python import log

from tasker import tasks
from tasker.utilities import db_keepalive, redis_connection

splash = """ 
                          .".
                         /  |
                        /  /
                       / ,"
           .-------.--- /
          "._ __.-/ o. o\  
             "   (    Y  )
                  )     /
                 /     (
                /       Y      ID: {idx}
            .-"         |      Handlers available: {handler_count}
           /  _     \    \     Redis host: {redis_host}
          /    `. ". ) /' )    Redis port: {redis_port}
         Y       )( / /(,/     Tasker DB: {redis_tasker_db}
        ,|      /     )        
       ( |     /     /         [{message}]
        " \_  (__   (__        
            "-._,)--._,)
\r\n
"""


class TaskerEngine(service.Service):
    HEARTBEAT_INTERVAL = 1  # Using the heartbeat to update the mothership
    IDLE_TIMEOUT = 10
    WORKER_TIMEOUT = 10
    LISTEN_LOOP_IDLE_INTERVAL = 0.1

    idx = None
    handler = None
    rconn = None

    _last_action_time = None
    _tasks_completed = 0

    _idle_sleep_time = time.time()
    _idle_previous_state = True  # Starting out as idle
    _idle_wakeup_time = None

    def __init__(self):
        self.idx = str(uuid.uuid4())
        self._last_action_time = time.time() - self.IDLE_TIMEOUT
        self.rconn = redis_connection()
        self.update_mothership()

        signal.signal(signal.SIGINT, self.interrupt)

    def startService(self):
        log.msg("Setting up tasker engine...")

        call_later(0, self.heartbeat)

        self.delayed_listen_loop()
        self.launch_workers()

        log.msg(splash.format(
            **{'idx': self.idx, 'handler_count': len(tasks.Task.handlers),
               'message': 'tasker ready for action',
               'redis_host': settings.REDIS_HOST,
               'redis_port': settings.REDIS_PORT,
               'redis_tasker_db': settings.REDIS_TASKER_DB}))
        #
        # tasks.Task.create(tasks.SCRAPE_AUTOTRACK)

    def interrupt(self, signum, frame):
        log.msg('\x1b[0;37;41m' + "Caught signal: %s" % signum + '\x1b[0m')
        reactor.stop()

    def launch_workers(self):
        for n in range(1):
            # log.msg("Launched worker %s" % n)
            defer_to_thread(self.worker_loop, n)

    def print_and_reset_worker_log(self):
        processing_time = \
            (self._idle_sleep_time - self._idle_wakeup_time) - self.IDLE_TIMEOUT

        log.msg("Processing time: %s" % round(processing_time))
        log.msg("Tasks completed: %s" % self._tasks_completed)
        log.msg("Tasks per second: %s" % round((self._tasks_completed /
                                                processing_time)))

        self._tasks_completed = 0

    def check_for_idle(self):
        new_idle_state = False

        if (time.time() - (self._last_action_time or time.time())) > \
                self.IDLE_TIMEOUT:
            new_idle_state = True

        if self._idle_previous_state is not new_idle_state:
            self._idle_previous_state = new_idle_state
            if new_idle_state:
                self._idle_sleep_time = time.time()
                log.msg('\x1b[0;37;41m' + "Went idle..." + '\x1b[0m')

                self.print_and_reset_worker_log()
            else:
                self._idle_wakeup_time = time.time()
                log.msg('\x1b[6;30;42m' + "Became active!" + '\x1b[0m')

        return new_idle_state

    def generate_signature(self):
        """ Signature is used to keep track of workers in the mothership
        """
        return {
            'last_heartbeat': time.time(),
            'tasks_completed': self._tasks_completed,
            'last_action_time': self._last_action_time,
        }

    def update_mothership(self):
        self.rconn.hset(settings.TASKER_WORKER_REGISTRATION_KEY,
                        self.idx, json.dumps(self.generate_signature()))

        workers = self.rconn.hgetall(
            settings.TASKER_WORKER_REGISTRATION_KEY)

        for idx, data in workers.items():
            data = json.loads(data)

            if (time.time() - data.get('last_heartbeat')) > self.WORKER_TIMEOUT:
                log.msg("Worker timed out: %s" % idx)
                self.rconn.hdel(settings.TASKER_WORKER_REGISTRATION_KEY,
                                idx)

    def heartbeat(self):
        self.update_mothership()
        self.check_for_idle()

        call_later(self.HEARTBEAT_INTERVAL, self.heartbeat)

    def delayed_listen_loop(self):
        if tasks.Task.has_delayed_task_available():
            try:
                tasks.Task.queue_next_delayed_task(prioritize=True)
            except IndexError:
                log.err(
                    '\x1b[0;37;41m' + "Failed delaying next task."
                                      " Was presumably already picked "
                                      "up" + '\x1b[0m')

            return call_later(0, self.delayed_listen_loop)

        return call_later(self.LISTEN_LOOP_IDLE_INTERVAL,
                          self.delayed_listen_loop)

    def worker_loop(self, n):
        self.check_for_idle()

        task_data = tasks.Task.fetch_task()

        if not task_data:
            call_later(1, self.worker_loop, n)
            return

        self._last_action_time = time.time()

        try:
            self.process_task(task_data)
        except Exception as e:
            print('\x1b[0;37;41m' + "Task failed.. Continuing loop" + '\x1b[0m')

        call_later(0, self.worker_loop, n)

    def process_task(self, task_data):
        task_type = task_data['type']

        if task_type not in tasks.Task.handlers:
            log.msg('\x1b[0;37;41m' + "Handler not found for task_type: %s"
                    % task_type + '\x1b[0m')
            return

        task_kwargs = task_data.get('data', None)

        for handler in tasks.Task.handlers.get(task_type):
            db_keepalive()

            try:
                if not task_kwargs:
                    log.msg(
                        '\x1b[0;37;41m' + "No data found in work message: %s"
                        % task_type + '\x1b[0m')
                    handler()
                else:
                    handler(**task_kwargs)
            except Exception as e:
                log.err(e)

                raise

            self._tasks_completed += 1
            self._last_action_time = time.time()
