#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'king'


from fabric.api import *

env.user = 'hadoop'
env.password = 'hadoop'

env.roledefs = {
    'test': ['10.214.208.11']
}

env.spark = 'node1'



@roles('test')
def dpconsumer():
    with settings(warn_only=True):
        result = put("wkdown.py", '/tmp/wkdown.py')
    if result.failed and not confirm("put jar file failed, Continue[Y/N]"):
        abort("Aborting file put jar task!")
    run('/usr/local/spark/bin/spark-submit --master spark://%s:7077 --total-executor-cores 4 --num-executors 1 --executor-memory 2G /tmp/wkdown.py' % (env.spark))

@task
def deploy():
    execute(dpconsumer)