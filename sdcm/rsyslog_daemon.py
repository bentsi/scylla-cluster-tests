import os.path
from logging import getLogger
from sdcm.remote import LocalCmdRunner
from tempfile import mkstemp
import getpass

from sdcm.utils import get_free_port

LOGGER = getLogger(__name__)


RSYSLOG_DOCKER_ID = None
RSYSLOG_CONF = """
global(processInternalMessages="on")

module(load="builtin:omfile" fileOwner="{owner}" dirOwner="{owner}")
#module(load="imtcp" StreamDriver.AuthMode="anon" StreamDriver.Mode="1")
module(load="impstats") # config.enabled=`echo $ENABLE_STATISTICS`)
module(load="imrelp")
module(load="imptcp")
module(load="imudp" TimeRequery="500")
module(load="omstdout")
module(load="mmjsonparse")
module(load="mmutf8fix")

input(type="imptcp" port="{port}")
# input(type="imudp" port="{port}")
# input(type="imrelp" port="1601")

# includes done explicitely
include(file="/etc/rsyslog.conf.d/log_to_logsene.conf" config.enabled=`echo $ENABLE_LOGSENE`)
include(file="/etc/rsyslog.conf.d/log_to_files.conf" config.enabled=`echo $ENABLE_LOGFILES`)

#################### default ruleset begins ####################

# we emit our own messages to docker console:
syslog.* :omstdout:

include(file="/config/droprules.conf" mode="optional")  # this permits the user to easily drop unwanted messages

action(name="main_utf8fix" type="mmutf8fix" replacementChar="?")

include(text=`echo $CNF_CALL_LOG_TO_LOGFILES`)
include(text=`echo $CNF_CALL_LOG_TO_LOGSENE`)

"""


def generate_conf_file(port):
    file_obj, conf_path = mkstemp(prefix="sct-rsyslog", suffix=".conf")
    file_obj.write(RSYSLOG_CONF.format(port=port, owner=getpass.getuser()))
    file_obj.close()
    LOGGER.debug("Rsyslog conf file created in '%s'", conf_path)
    return conf_path

# TODO: call start_rsyslog from tester, and pass the port to each node


def start_rsyslog(docker_name, log_dir):
    """
        log_dir: directory where to store the logs
    """
    global RSYSLOG_DOCKER_ID

    log_dir = os.path.abspath(log_dir)
    port = get_free_port()
    conf_path = generate_conf_file(port)
    local_runner = LocalCmdRunner()
    res = local_runner.run('''
        mkdir -p {log_dir};
        docker run --rm -d \
        -v ${{HOME}}:${{HOME}} \
        -v {log_dir}:/logs
        -v {conf_path}:/etc/rsyslog.conf \
        -p {port}:{port} \
        -it  --name {name} rsyslog/syslog_appliance_alpine
    '''.format(**locals()))

    RSYSLOG_DOCKER_ID = res.stdout.strip()
    LOGGER.info("Rsyslog started. Container id: %s", RSYSLOG_DOCKER_ID)
    return port


def stop_rsyslog():
    if RSYSLOG_DOCKER_ID:
        local_runner = LocalCmdRunner()
        local_runner.run("docker kill {id}".format(id=RSYSLOG_DOCKER_ID), ignore_status=True)
