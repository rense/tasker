#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='tasker',
    version='0.1',
    description='Python Distribution Utilities',
    author='Tom Reitsma',
    author_email='tom@triton-it.nl',
    url='https://github.com/tomreitsma/tasker',
    packages=find_packages(),
    install_requires=[
        'redis',
        'twisted',
        'django',
    ],
    entry_points={
        'console_scripts': ['tasker=tasker.runner:main'],
    }
)
