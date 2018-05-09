import os
import errno
from server.server import Client
from textwrap import dedent
from logging import getLogger
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from sdcm.utils import retrying, log_run_info
from sdcm.cluster import BaseNode
from thrift.transport.TTransport import TTransportException
from contextlib import contextmanager
try:
    from thrift.protocol import fastbinary
except:
    fastbinary = None

logger = getLogger(__name__)


class CharybdeFS(Client):
    VERSION = "0.1"
    SCYLLA_DIR = "/var/lib/scylla"
    INSTALL_DIR = os.path.join(SCYLLA_DIR, "charybdefs-master")

    def __init__(self, node, target_dir, port=9090):
        self.node = node  # type: BaseNode
        self.target_dir = target_dir
        self.port = port
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(self.node.public_ip_address, self.port))
        Client.__init__(self, TBinaryProtocol.TBinaryProtocol(self.transport))
        # didn't use super() since Base class is old style class (doesn't inherit from object)

    @retrying(n=2, sleep_time=3, allowed_exceptions=(TTransportException,))
    def connect(self):
        self.transport.open()
        self._validate_version()
        logger.info("Connected to CharybdeFS.")

    def disconnect(self):
        self.transport.close()

    def _validate_version(self):
        logger.info("Checking CharybdeFS version...")
        res = self.node.remoter.run(cmd="cat %s" % os.path.join(self.INSTALL_DIR, "VERSION"))
        assert self.VERSION == res.stdout.strip(), "Client and server versions don't match. " \
                                                   "Python thrift code should be regenerated and commited to SCT repo"

    @log_run_info
    def install(self):
        self.node.stop_scylla()
        script = dedent(""" 
            yum install -y gcc-c++ cmake fuse fuse-devel thrift python-thrift thrift-devel unzip              
            cd {0.SCYLLA_DIR}
            wget https://github.com/scylladb/charybdefs/archive/master.zip -O master.zip
            unzip -o master.zip
            cd {0.INSTALL_DIR}
            thrift -r --gen cpp --gen py server.thrift
            cmake CMakeLists.txt
            make            
            mv {0.target_dir} {0.target_dir}-charybde
            mkdir -p {0.target_dir}
            chown scylla:scylla {0.target_dir}
            echo user_allow_other > /etc/fuse.conf            
            touch installed
        """.format(self))
        self.node.run(cmd="sudo bash -cxe '%s'" % script)
        self.node.run(cmd="cd {0.INSTALL_DIR} && sudo ./charybdefs {0.target_dir} -omodules=subdir,subdir={0.target_dir}-charybde,allow_other,nonempty".format(self))
        assert self.node.is_port_used(port=self.port, service_name="CharybdeFS"), "CharybdeFS process failed to start!"
        self.node.start_scylla()

    @contextmanager
    def inject_syscall_error(self, syscall, err_code):
        err_code_str = errno.errorcode[err_code]
        err_msg = os.strerror(err_code)
        logger.info("Going to inject {err_code_str} on {syscall} : {err_msg}".format(**locals()))
        self.set_fault(methods=[syscall], random=False, err_no=err_code, probability=100000, regexp="",
                       kill_caller=False, delay_us=0, auto_delay=False)
        yield
        logger.debug("Clearing faults on '%s'", syscall)
        self.clear_fault(method=syscall)
        self.node.stop_scylla()
        self.node.start_scylla()
