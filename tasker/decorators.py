from tasker.task import Task


class listens_to:
    def __init__(self, task_type):
        self.task_type = task_type

    def __call__(self, f):
        if self.task_type not in Task.handlers:
            Task.handlers[self.task_type] = []
        Task.handlers[self.task_type].append(f)

        # log.msg("Registered handler for %s" % self.task_type)

        return f
