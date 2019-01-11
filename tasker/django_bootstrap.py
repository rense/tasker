import os

base_dir = os.path.dirname(os.path.abspath('__file__'))

with open(os.path.join(base_dir, 'environment'), 'r') as _file:
    environment = _file.read().replace('\n', '')

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings.%s" % environment)

import django
django.setup()

print("Bootstrapped django v%s" % django.__version__)
