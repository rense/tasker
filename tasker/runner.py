def main():
    from twisted.scripts.twistd import run
    from os.path import join, dirname
    from sys import argv
    argv[1:] = [
        '-y', join(dirname(__file__), "tasker.tac"),
        '--pidfile', '/var/run/myapp.pid',
        '--logfile', '/var/run/myapp.log'
    ]
    run()
