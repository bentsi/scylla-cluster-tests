import errno
from avocado import main
from cassandra.cluster import HostDistance

from charybdefs import CharybdeFS
from cassandra.connection import OperationTimedOut
from sdcm.tester import ClusterTester, clean_resources_on_exception
from sdcm.utils import get_random_string

class ScyllaState(object):
    def __init__(self, is_server_process_running=False, is_jmx_process_running=False, cql_port_up=True, log_has_err_msg=""):
        self.is_server_process_running = is_server_process_running
        self.is_jmx_process_running = is_jmx_process_running
        self.cql_port_up = cql_port_up
        self.err_msg = log_has_err_msg

    def has_msg(self, database_log_obj):
        for line in database_log_obj.readlines():
            if self.err_msg in line:
                print "Found '%s' in '%s'" % (self.err_msg, line)
                return self.err_msg
        return ""

    def verify_on(self, node, database_log_obj):
        # TODO: check nodetool status
        errors = []
        current_state = dict(is_server_process_running=node.is_service_up("scylla-server"),
                             is_jmx_process_running=node.jmx_up(),
                             cql_port_up=node.is_port_used(port=node.cql_port, service_name="CQL endpoint"),
                             err_msg=self.has_msg(database_log_obj))
        for state_name, state_val in current_state.iteritems():
            expected_state_val = getattr(self, state_name)
            if expected_state_val != state_val:
                errors.append("Expected {state_name}:{expected_state_val}. Got {state_name}:{state_val}".format(**locals()))
        assert not errors, "State verification failed!\n%s" % "\n".join(errors)


class CharybdeTester(ClusterTester):
    """
        Inject FileSystem faults using CharybdeFS
        :avocado: enable
    """
    @clean_resources_on_exception
    def setUp(self):
        super(CharybdeTester, self).setUp()
        self.target_node = self.db_cluster.get_random_node(exclude_seed=True)
        self.database_log_obj = open(self.target_node.database_log, "r")  # this is ugly and should be rewritten when
                                                                          # events infra will be implemented
        self.charybdefs = CharybdeFS(node=self.target_node, target_dir="/var/lib/scylla/data")
        self.charybdefs.install()
        self.charybdefs.connect()
    
    def tearDown(self):
        self.charybdefs.disconnect()
        super(CharybdeTester, self).tearDown()

    def create_keyspace(self):
        session = self.cql_connection_exclusive(node=self.target_node)
        # session.cluster.load_balancing_policy.distance = lambda _: HostDistance.IGNORED
        keyspace_name ="eio_test_" + get_random_string().lower()
        self.create_ks(session=session, name=keyspace_name, rf=3)

    def run_io_pattern(self, pat_func, expected_exceptions=(Exception,)):
        try:
            pat_func()
        except expected_exceptions as e:
            self.log.debug("Expected exception(s): %s" % e)
        else:
            self.fail("IO pattern should fail with exception %s but didn't fail" % expected_exceptions)

    def ftruncate_eio_create_keyspace(self):
        with self.charybdefs.inject_syscall_error(syscall="ftruncate", err_code=errno.EIO):
            self.run_io_pattern(pat_func=self.create_keyspace, expected_exceptions=(OperationTimedOut,))
            expected_state = ScyllaState(is_server_process_running=True,
                                         is_jmx_process_running=True,
                                         cql_port_up=True,  # Should be False. Details in issue #3406
                                         log_has_err_msg="storage_io_error (Storage I/O error: 5: Input/output error)")
            expected_state.verify_on(node=self.target_node, database_log_obj=self.database_log_obj)

    def ftruncate_enospc_create_keyspace(self):
        with self.charybdefs.inject_syscall_error(syscall="ftruncate", err_code=errno.ENOSPC):
            self.run_io_pattern(pat_func=self.create_keyspace, expected_exceptions=(OperationTimedOut,))
            expected_state = ScyllaState(is_server_process_running=True,
                                         is_jmx_process_running=True,
                                         cql_port_up=True,  # Should be False. Details in issue #3406
                                         log_has_err_msg="storage_io_error (Storage I/O error: 28: No space left on device)")
            expected_state.verify_on(node=self.target_node, database_log_obj=self.database_log_obj)

    def test_all(self):
        self.ftruncate_eio_create_keyspace()
        self.ftruncate_enospc_create_keyspace()


if __name__ == '__main__':
    main()
