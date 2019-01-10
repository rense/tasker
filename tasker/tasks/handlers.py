from tasker import tasks
from tasker.decorators import listens_to


@listens_to(tasks.TEST_TASK_A)
def test_task_a(argument_a=None):
    print(argument_a)
