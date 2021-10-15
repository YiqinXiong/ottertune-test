import importlib
import os

from fabric.api import local, task

# Some FIXTURES
SCRIPT_DIR = '/data1/workspace/ottertune-test/script'


@task
def run_sql_script(scriptfile, *args):
    local_path = os.path.join(SCRIPT_DIR, scriptfile)
    res = local('setsid sh {} {}'.format(local_path, ' '.join(args)))
    return res


@task
def file_exists_local(filename):
    return os.path.exists(filename)


class FabricException(Exception):
    pass
