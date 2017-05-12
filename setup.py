#!/usr/bin/env python

from distutils.core import setup

setup(
    name='The El',
    version='0.1dev',
    packages=['the_el',],
    install_requires=[
        'boto3==1.4.4',
        'click==6.7',
        'sqlalchemy>=1.0,<2.0a',
        'smart_open==1.5.2',
        'tableschema==1.0.0a4'
    ],
    entry_points={
        'console_scripts': [
            'the_el=the_el:main',
        ],
    },
)
