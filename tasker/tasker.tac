#!/usr/bin/env python
import sys
import os

os.system('clear')

EXTRA_PATHS = (
    '.',
)

for p in EXTRA_PATHS:
    sys.path.append('%s' % os.path.join(p))

# import django_bootstrap

from tasker.engine import TaskerEngine

from twisted.application.service import Application
from twisted.application import service

from twisted.python.log import ILogObserver, FileLogObserver
from twisted.python.logfile import DailyLogFile

application = Application("MatchmakerService")

# logfile = DailyLogFile("matchmaker.log", "logs/")
# application.setComponent(ILogObserver, FileLogObserver(logfile).emit)

main = service.MultiService()

pm_service = TaskerEngine()
pm_service.setName('MatchmakerService')
pm_service.setServiceParent(main)

main.setServiceParent(application)
