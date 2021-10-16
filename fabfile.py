#
# OtterTune - fabfile.py
#
# Copyright (c) 2017-18, Carnegie Mellon University Database Group
#
'''
Created on Mar 23, 2018

@author: bohan
'''
import glob
import json
import os
import re
import time
import toml
from collections import OrderedDict
from multiprocessing import Process

import logging
from logging.handlers import RotatingFileHandler

import requests
from fabric.api import env, lcd, local, settings, task
from fabric.state import output as fabric_output

from utils import (run_sql_script, FabricException, file_exists_local)

# Some FIXTURES
WORKLOADS = ['tpcc', 'tpch', 'tatp',
             'wikipedia', 'resourcestresser', 'twitter',
             'epinions', 'ycsb', 'seats',
             'auctionmark', 'chbenchmark', 'voter',
             'sibench', 'noop', 'smallbank',
             'hyadapt']
CLUSTERS = ['tidb-1',
            'tidb-2',
            'tidb-3']
HOSTS = ['tidb-pd-1',
         'tidb-pd-2',
         'tidb-pd-3']
BENCHBASE_HOME = '/data1/workspace/benchbase/target/benchbase-2021-SNAPSHOT'
BENCHBASE_LOG_DIR = os.path.join(BENCHBASE_HOME, 'log')
TEST_HOME = os.path.dirname(os.path.realpath(__file__))
TEST_LOG_DIR = os.path.join(TEST_HOME, 'log')
TEST_LOG_PATH = os.path.join(TEST_LOG_DIR, 'test.log')

# Fabric settings
fabric_output.update({
    'running': True,
    'stdout': True,
})
env.abort_exception = FabricException
env.hosts = ['localhost']
# xyq add
env.warn_only = True
env.password = ''
# xyq add

# Create local directories
for _d in (BENCHBASE_LOG_DIR, TEST_LOG_DIR):
    os.makedirs(_d, exist_ok=True)

# Configure logging
LOG = logging.getLogger(__name__)
LOG.setLevel(getattr(logging, 'DEBUG', logging.DEBUG))
Formatter = logging.Formatter(  # pylint: disable=invalid-name
    fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S')
ConsoleHandler = logging.StreamHandler()  # pylint: disable=invalid-name
ConsoleHandler.setFormatter(Formatter)
LOG.addHandler(ConsoleHandler)
FileHandler = RotatingFileHandler(  # pylint: disable=invalid-name
    TEST_LOG_PATH, maxBytes=50000, backupCount=2)
FileHandler.setFormatter(Formatter)
LOG.addHandler(FileHandler)


@task
def get_cluster_name_and_host(bench_type):
    if bench_type not in WORKLOADS:
        raise Exception(f"Workload {bench_type} Not Supported !")
    idx = WORKLOADS.index(bench_type) % len(CLUSTERS)
    return CLUSTERS[idx], HOSTS[idx]


@task
def restart_database(db_seq_num):
    cluster_name, _ = get_cluster_name_and_host(db_seq_num)
    run_sql_script('reload_tidb_cnf.sh', cluster_name)


@task
def drop_database(bench_type):
    # local(
    #     "mysql --user={} --password={} -h {} -P {} -e 'drop database if exists {}'".format(dconf.DB_USER,
    #                                                                                        dconf.DB_PASSWORD,
    #                                                                                        dconf.DB_HOST,
    #                                                                                        dconf.DB_PORT,
    #                                                                                        dconf.DB_NAME))
    cluster_name, host_name = get_cluster_name_and_host(bench_type)

    # local("tiup cluster clean {} --all --ignore-role prometheus --yes".format(dconf.TIDB_CLUSTER_NAME))
    # local("tiup cluster start {}".format(dconf.TIDB_CLUSTER_NAME))
    run_sql_script('clean_tidb_data.sh', cluster_name)
    run_sql_script('start_cluster.sh', cluster_name)




@task
def create_database(bench_type):
    _, host_name = get_cluster_name_and_host(bench_type)
    local("mysql --user={} --password={} -h {} -P {} -e 'create database if not exists {}'".format('root',
                                                                                       '',
                                                                                       host_name,
                                                                                       '4000',
                                                                                       bench_type))


@task
def change_conf(bench_type):
    cluster_name, host_name = get_cluster_name_and_host(bench_type)
    local('cp {0} {0}.bak'.format(f'./script/{cluster_name}/config.yaml'))

    run_sql_script('change_tidb_cnf.sh', cluster_name, f'./script/{cluster_name}/config.yaml')


@task
def load_benchbase_bg(bench_type):

    create_database(bench_type)

    config_path = os.path.join(BENCHBASE_HOME, f'config/tidb/{bench_type}_config.xml')
    log_path = os.path.join(BENCHBASE_HOME, f'log/{bench_type}_load.log')
    cmd = "java -jar benchbase.jar -b {} -c {} --create=true --load=true > {} 2>&1 &". \
        format(bench_type, config_path, log_path)
    with lcd(BENCHBASE_HOME):  # pylint: disable=not-context-manager
        local(cmd)


@task
def run_benchbase_bg(bench_type):
    config_path = os.path.join(BENCHBASE_HOME, f'config/tidb/{bench_type}_config.xml')
    log_path = os.path.join(BENCHBASE_HOME, f'log/{bench_type}_run.log')
    cmd = "java -jar benchbase.jar -b {} -c {} --execute=true -s 5 > {} 2>&1 &". \
        format(bench_type, config_path, log_path)
    with lcd(BENCHBASE_HOME):  # pylint: disable=not-context-manager
        local(cmd)


@task
def dump_database(bench_type):
    DB_DUMP_DIR = f'/data1/{bench_type}'
    _, host_name = get_cluster_name_and_host(bench_type)
    LOG.info('Dump database %s to %s by using tiup dumpling', bench_type, DB_DUMP_DIR)
    local('tiup dumpling -u {} --host {} -P {} -F {} -t {} -o {}'.format('root',
                                                                         host_name,
                                                                         '4000',
                                                                         '256MiB',
                                                                         '16',
                                                                         DB_DUMP_DIR))


@task
def restore_database(bench_type):
    cluster_name, host_name = get_cluster_name_and_host(bench_type)
    DB_DUMP_DIR = f'/data1/{bench_type}'
    TIDB_LIGHTNING_CONF = os.path.join(TEST_HOME, f'script/{cluster_name}/tidb-lightning.toml')

    # 改变lightning的数据源目录，和负载类型保持一致
    tidb_lightning_conf_dict = toml.load(TIDB_LIGHTNING_CONF)
    tidb_lightning_conf_dict['mydumper']['data-source-dir'] = DB_DUMP_DIR
    with open(TIDB_LIGHTNING_CONF, 'w') as f:
        toml.dump(tidb_lightning_conf_dict, f)

    dumpfile = os.path.join(DB_DUMP_DIR, bench_type + '.dump')
    if not file_exists_local(
            os.path.join(DB_DUMP_DIR, bench_type + '-schema-create.sql')):
        raise FileNotFoundError("Database dumpfile '{}' does not exist!".format(dumpfile))

    LOG.info('Start restoring database')

    # 导入前先删除原有数据库
    drop_database(bench_type)
    LOG.info('Database %s has been dropped before restore %s.', bench_type, dumpfile)
    # 然后使用tidb_lightning导入数据
    lightning_checkpoint_path = f'/tmp/{cluster_name}_lightning_checkpoint.pb'
    if file_exists_local(lightning_checkpoint_path):
        local(f"rm -rf {lightning_checkpoint_path}")
    res = run_sql_script('run_tidb_lightning.sh', TIDB_LIGHTNING_CONF)
    err_times = 0
    while res.failed and err_times < 3:
        # run_sql_script('tidb_lightning_switch_mode.sh')
        drop_database(bench_type)
        LOG.info('Database %s has been dropped before restore %s.', bench_type, dumpfile)
        local(f"rm -rf {lightning_checkpoint_path}")
        LOG.info('Cleaned tidb_lightning error check points! Now try it again...')
        res = run_sql_script('run_tidb_lightning.sh', TIDB_LIGHTNING_CONF)
        err_times += 1

    LOG.info('Wait %s seconds after restoring database', 30)
    time.sleep(30)
    LOG.info('Finish restoring database')


@task
def clean_conf(bench_type):
    cluster_name, _ = get_cluster_name_and_host(bench_type)
    run_sql_script('clean_tidb_cnf.sh', cluster_name)
    LOG.info('config cleaned!!! all configs are reseted to default.')


@task
def run(bench_type):
    # 导入测试数据（需保证数据存在、lightning toml中data_dir正确）
    restore_database(bench_type)
    # 运行测试，结果导出在BENCHBASE_HOME/log/{bench_type}_run.log
    run_benchbase_bg(bench_type)

@task
def load(bench_type):
    # 导入前先删除原有数据库
    drop_database(bench_type)
    # 执行load操作
    load_benchbase_bg(bench_type)

