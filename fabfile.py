#
# OtterTune - fabfile.py
#
# Copyright (c) 2017-18, Carnegie Mellon University Database Group
#
'''
Created on Mar 23, 2018

@author: bohan
'''
import datetime
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
             'smallbank', 'sysbench', 'ycsb']
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
SYSBENCH_RUN_TYPE = ['oltp_point_select', 'oltp_update_index', 'oltp_read_only', 'oltp_read_write', 'oltp_write_only',
                     'select_random_points', 'select_random_ranges', 'bulk_insert', 'oltp_insert']

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
def drop_database(cluster_name):
    # local(
    #     "mysql --user={} --password={} -h {} -P {} -e 'drop database if exists {}'".format(dconf.DB_USER,
    #                                                                                        dconf.DB_PASSWORD,
    #                                                                                        dconf.DB_HOST,
    #                                                                                        dconf.DB_PORT,
    #                                                                                        dconf.DB_NAME))

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
def load_benchbase_bg(bench_tool, bench_type, cluster_name=''):
    create_database(bench_type)

    if bench_type == 'sysbench':
        config_path = os.path.join(BENCHBASE_HOME, f'config/tidb/{cluster_name}/{bench_type}_config_load')
        log_path = os.path.join(BENCHBASE_HOME, f'log/{bench_type}_load.log')
        cmd = f"sysbench --config-file={config_path} oltp_point_select " \
              f"--tables=32 --table-size=10000000 prepare > {log_path} 2>&1 &"
        local(cmd)
    else:
        config_path = os.path.join(BENCHBASE_HOME, f'config/tidb/{bench_type}_config.xml')
        log_path = os.path.join(BENCHBASE_HOME, f'log/{bench_type}_load.log')
        cmd = f"java -jar benchbase.jar -b {bench_type} -c {config_path} --create=true --load=true > {log_path} 2>&1 &"
        with lcd(BENCHBASE_HOME):  # pylint: disable=not-context-manager
            local(cmd)


@task
def run_benchbase_bg(bench_tool, bench_type, cluster_name='', sysbench_run_type='', tiupbench_con=1000, params=''):
    if bench_tool not in ['benchbase', 'tiupbench']:
        raise Exception(f"Bench tool {bench_tool} Not Supported !")
    if cluster_name != '' and cluster_name not in CLUSTERS:
        raise Exception(f"Cluster name {cluster_name} Not Supported !")

    if bench_type == 'sysbench':
        if sysbench_run_type not in SYSBENCH_RUN_TYPE:
            raise Exception(f"Sysbench run type {sysbench_run_type} Not Supported !")
        config_path = os.path.join(BENCHBASE_HOME, f'config/tidb/{cluster_name}/{bench_type}_config_run')
        log_path = os.path.join(BENCHBASE_HOME, f'log/{bench_type}_run_{sysbench_run_type}.log')
        cmd = f"sysbench --config-file={config_path} {sysbench_run_type} " \
              f"--tables=32 --table-size=10000000 {params} run > {log_path} 2>&1"
        LOG.info(f'Run test:\n {cmd}.')
        local(cmd)
        # 移动日志位置
        result_dir = os.path.join(BENCHBASE_HOME, 'results')
        nowtime = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        local(f'cp {config_path} {result_dir}/{bench_type}_{nowtime}.{sysbench_run_type}.config')
        local(f'cp {log_path} {result_dir}/{bench_type}_{nowtime}.{sysbench_run_type}.log')
    else:
        if bench_tool == 'benchbase':
            if cluster_name == '':
                config_path = os.path.join(BENCHBASE_HOME, f'config/tidb/{bench_type}_config.xml')
            else:
                config_path = os.path.join(BENCHBASE_HOME, f'config/tidb/{cluster_name}/{bench_type}_config.xml')

            log_path = os.path.join(BENCHBASE_HOME, f'log/{bench_type}_run.log')
            cmd = f"java -jar benchbase.jar -b {bench_type} -c {config_path} --execute=true -s 5 > {log_path} 2>&1 &"
            with lcd(BENCHBASE_HOME):  # pylint: disable=not-context-manager
                local(cmd)
        else:
            run_sql_script('tiup_bench_run.sh', HOSTS[CLUSTERS.index(cluster_name)], '4000', 'tpcc', '1000',
                           f'{tiupbench_con}',
                           '1h')


@task
def dump_database(bench_type, cluster_name=''):
    if cluster_name != '' and cluster_name not in CLUSTERS:
        raise Exception(f"Cluster name {cluster_name} Not Supported !")
    DB_DUMP_DIR = f'/data1/{bench_type}'
    host_name = HOSTS[CLUSTERS.index(cluster_name)]
    LOG.info('Dump database %s to %s by using tiup dumpling', bench_type, DB_DUMP_DIR)
    local('tiup dumpling -u {} --host {} -P {} -F {} -t {} -o {}'.format('root',
                                                                         host_name,
                                                                         '4000',
                                                                         '256MiB',
                                                                         '16',
                                                                         DB_DUMP_DIR))


@task
def restore_database(bench_tool, bench_type, cluster_name=''):
    if bench_tool not in ['benchbase', 'tiupbench']:
        raise Exception(f"Bench tool {bench_tool} Not Supported !")
    if cluster_name == '':
        cluster_name, _ = get_cluster_name_and_host(bench_type)
    elif cluster_name not in CLUSTERS:
        raise Exception(f"Cluster name {cluster_name} Not Supported !")
    DB_DUMP_DIR = f'/data1/{bench_type}' if bench_tool == 'benchbase' else f'/data1/{bench_type}-tiupbench'
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
    drop_database(cluster_name)
    LOG.info('Database %s has been dropped before restore %s.', bench_type, dumpfile)
    # 然后使用tidb_lightning导入数据
    lightning_checkpoint_path = f'/tmp/{cluster_name}_lightning_checkpoint.pb'
    if file_exists_local(lightning_checkpoint_path):
        local(f"rm -rf {lightning_checkpoint_path}")
    res = run_sql_script('run_tidb_lightning.sh', TIDB_LIGHTNING_CONF)
    err_times = 0
    while res.failed and err_times < 3:
        # run_sql_script('tidb_lightning_switch_mode.sh')
        drop_database(cluster_name)
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
def run(bench_tool, bench_type, cluster_name='', sysbench_run_type=''):
    if bench_tool not in ['benchbase', 'tiupbench']:
        raise Exception(f"Bench tool {bench_tool} Not Supported !")
    # 导入测试数据（需保证数据存在、lightning toml中data_dir正确）
    restore_database(bench_tool, bench_type, cluster_name)
    # 运行测试，结果导出在BENCHBASE_HOME/log/{bench_type}_run.log
    run_benchbase_bg(bench_tool, bench_type, cluster_name, sysbench_run_type)


@task
def load(bench_tool, bench_type, cluster_name=''):
    # 导入前先删除原有数据库
    drop_database(bench_type)
    # 执行load操作
    load_benchbase_bg(bench_tool, bench_type, cluster_name)


@task
def run_test_curr(bench_tool='benchbase', bench_type='sysbench', cluster_name='tidb-2',
                  sysbench_run_type='select_random_ranges', params='--delta=5000000 --time=540'):
    val_list = [30, 15, 8, 4, 2, 1]
    host_name = HOSTS[CLUSTERS.index(cluster_name)]
    for val in val_list:
        local("mysql --user={} --password={} -h {} -P {} -e 'set @@global.tidb_distsql_scan_concurrency={};'".format(
            'root',
            '',
            host_name,
            '4000',
            val))
        LOG.info(f'global.tidb_distsql_scan_concurrency changed to {val}.')
        time.sleep(20)
        run_benchbase_bg(bench_tool, bench_type, cluster_name, sysbench_run_type, params=params)
        if val != val_list[-1]:
            time.sleep(40)


@task
def run_with_patch(times):
    assert isinstance(times, int)
    patch_dir = '/data1/workspace'
    patch_paths = [os.path.join(patch_dir, 'tikv-server-master-old.tar.gz'),
                   os.path.join(patch_dir, 'tikv-server-optimizer-2.tar.gz')]
    for t in range(times):
        # test
        for patch_path in patch_paths:
            run_sql_script('patch_tikv.sh', 'tidb-1', patch_path)
            time.sleep(30)
            run_benchbase_bg('benchbase', 'tpcc', 'tidb-1')
            time.sleep(30)
