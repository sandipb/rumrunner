from __future__ import print_function
from __future__ import absolute_import

import logging
import time
import threading
import collections

try:
    import ujson as json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        import json
import zmq

logger = logging.getLogger(__name__)

# Socket for communication between rumrunner instances and its agent
AGENT_SOCKET = "inproc://rumrunner_agent"

# How long does the agent block on the receive socket before attempting to flush
AGENT_POLL_MILLIS = 500

# how long to consolidate values before sending to speakeasy
AGENT_FLUSH_INTERVAL_SECS = 5

# If the sending socket has these many events in queue, it will keep dropping further events
DEFAULT_HWM = 10000

METRIC_SUM, METRIC_COUNT = 0, 1


class Rumrunner(object):
    MOCK = False

    def __init__(self, metric_socket, app_name, hwm=5000, block=False,
                 strict_check_socket=True, start_agent=True,
                 agent_socket=AGENT_SOCKET,
                 flush_interval=AGENT_FLUSH_INTERVAL_SECS):
        self.metric_socket = metric_socket
        self.hwm = hwm
        self.flush_interval = flush_interval
        self.app_name = app_name
        self.block = block
        self.agent_quit = threading.Event()
        self.context = zmq.Context.instance()
        self.agent_socket = agent_socket

        self.setup_agent()

        # Send metrics
        self.send_socket = self.context.socket(zmq.PUSH)
        self.send_socket.set_hwm(hwm)
        self.send_socket.connect(self.agent_socket)
        self.send_socket.setsockopt(zmq.LINGER, 0)

        # start the agent
        if self.agent and start_agent:
            self.start_agent()

    def setup_agent(self):
        "Stores an agent instance in self.agent if all is good, else None"
        try:
            self.agent_quit.clear()
            self.agent = RumrunnerAgent(self.metric_socket, self.agent_quit,
                                        self.hwm,
                                        flush_interval=self.flush_interval,
                                        agent_socket=self.agent_socket,
                                        block=self.block)
        except RumRunnerAgentError as e:
            logger.debug("Ignoring agent setup error: %s", e)
            self.agent = None

    def start_agent(self):
        "We only recreate agent if a previous instance was successfully created"
        if self.agent and self.agent.is_dead():
            logging.info("Attempting to recreate dead rumrunner agent")
            self.setup_agent()
        if self.agent is not None:
            threading.Thread(name="RumrunnerAgent", target=self.agent.run).start()

    def shutdown(self):
        "Shutdown the agent"
        self.agent_quit.set()

    def __new__(cls, *args, **kwargs):
        if cls.MOCK:
            return MockRumrunner(*args, **kwargs)
        else:
            return super(Rumrunner, cls).__new__(cls)

    def counter(self, metric_name, value=1):
        return self.send(metric_name, value, 'COUNTER')

    def gauge(self, metric_name, value):
        return self.send(metric_name, value, 'GAUGE')

    def percentile(self, metric_name, value):
        return self.send(metric_name, value, 'PERCENTILE')

    def send(self, metric_name, value, metric_type):
        try:
            datapoint = [self.app_name, metric_name, metric_type, value]
            if self.block:
                self.send_socket.send(json.dumps(datapoint))
            else:
                self.send_socket.send(json.dumps(datapoint), zmq.NOBLOCK)
            return True
        except zmq.error.Again as e:
            # Failed to send message
            logger.debug("Metric socket error - {0}".format(e))
            return False


class RumRunnerAgentError(Exception):
    pass


class RumrunnerAgent(object):
    """Receive data from all threads in the current process and send consolidated data to speakeasy

    There should be only one thread of this class running, as it uses data structures without locks.
    This is gated by the ownership of a zmq inproc socket in the global zmq context.
    """
    def __init__(self, metric_socket, quit_event,
                 hwm=DEFAULT_HWM, strict_check_socket=True,
                 flush_interval=AGENT_FLUSH_INTERVAL_SECS,
                 agent_socket=AGENT_SOCKET,
                 block=False):
        self.metric_socket = metric_socket
        self.quit_event = quit_event
        self.flush_interval = flush_interval
        self.last_run = time.time()
        self.agent_socket = agent_socket
        self.block = block

        context = zmq.Context.instance()
        self.metrics = {}

        # socket to receiving metrics from local rumrunner
        self.recv_socket = context.socket(zmq.PULL)
        try:
            self.recv_socket.bind(self.agent_socket)
        except zmq.ZMQError:
            raise RumRunnerAgentError(
                "Error binding to agent socket at " + self.agent_socket + ". Is actually normal if you have created "
                "more than one rumrunner instance in the same process")

        # socket to send metrics to speakeasy
        self.send_socket = context.socket(zmq.PUSH)
        self.send_socket.set_hwm(hwm)
        self.send_socket.connect('ipc://{0}'.format(self.metric_socket))
        self.send_socket.setsockopt(zmq.LINGER, 0)
        if strict_check_socket:
            self.test_socket_writable(strict_check_socket)

    def test_socket_writable(self, strict):
        "Send a confirmed write to the remote socket to verify the address works"
        tracker = self.send_socket.send('', copy=False, track=True)
        try:
            tracker.wait(3)
        except zmq.NotDone:
            raise RumRunnerAgentError('Metric socket not writable')

    def run(self):
        "Keep reading from socket till flush interval reached"
        if not self.recv_socket:
            raise Exception("No receive socket present (A shutdown agent cannot be restarted)")

        logging.debug("Starting rumrunner agent loop")
        while not self.quit_event.is_set():
            self._run_once()
        self.recv_socket.unbind(self.agent_socket)
        self.recv_socket = None
        logging.debug("Unbinding and exiting rumrunner agent loop")

    def is_dead(self):
        return self.recv_socket is None

    def _run_once(self):
        while time.time() < (self.last_run + self.flush_interval):
            events_waiting = self.recv_socket.poll(AGENT_POLL_MILLIS)
            if events_waiting:
                try:
                    self.process_data(self.recv_socket.recv())
                except Exception as e:
                    logging.error("Error aggregating data in RumrunnerAgent loop: %s", e)
                    continue
        try:
            self.send_metrics()
        except Exception as e:
            logging.error("Error sending data in RumrunnerAgent loop: %s", e)

    def init_app(self, app):
        self.metrics[app] = {
            'GAUGE': collections.defaultdict(lambda: [0, 0]),
            'COUNTER': collections.defaultdict(lambda: [0, 0]),
            'PERCENTILE': collections.defaultdict(list)
        }

    def process_data(self, data):
        "Process a data frame received in the incoming socket"
        try:
            if not data:  # empty packets
                logger.warn("Ignoring empty packet")
                return
            event = json.decode(data)
            app_name, metric_name, metric_type, metric_value = event
            if app_name not in self.metrics:
                self.init_app(app_name)
            # logging.info("Processing a=%s m=%s t=%s v=%f", app_name, metric_name, metric_type, metric_value)

            # we store the sum of the value received for a metric for gauges and counters,
            # as well as the number of such metrics received. For percentiles, we keep all
            # the values in a list. The overall idea is that only gauges and counters are to
            # be collapsed into a single data point, but the percentiles are passed on as-is.
            if metric_type in ("GAUGE", "COUNTER"):
                self.metrics[app_name][metric_type][metric_name][METRIC_SUM] += metric_value
                self.metrics[app_name][metric_type][metric_name][METRIC_COUNT] += 1
            elif metric_type == "PERCENTILE":
                self.metrics[app_name][metric_type][metric_name].append(metric_value)
            else:
                logger.error("Ignoring invalid metric type '%s' for app=%s, metric=%s",
                             metric_type, app_name, metric_name)
        except (ValueError, IndexError):
            logger.error("Ignoring malformed data which could not be decoded")
            return

    def collect_stats(self):
        """
        Collect stats about the number of metrics aggregate - at overall and app level::

            rumrunner._all_.COUNTER.unique_total  = number of unique counters emitted
            rumrunner._all_.COUNTER.aggregate_total  = number of counter values collapsed into the above unique values
            ...

        For percentiles, while the metrics are made available the same way, it should be noted
        that there is no collapse/aggregation, and so the metrics don't intend to show any savings,
        just occurences.
        """
        self.init_app("rumrunner")
        UNIQUE, AGGREGATE = 0, 1
        # overall metrics
        all_stats = {"COUNTER": [0, 0], "GAUGE": [0, 0], "PERCENTILE": [0, 0]}

        for app in self.metrics:
            # app level metrics
            app_stats = {"COUNTER": [0, 0], "GAUGE": [0, 0], "PERCENTILE": [0, 0]}
            if app == "rumrunner":  # the stats we show ignore our internal metrics
                continue
            for metric_type in self.metrics[app]:
                for metric in self.metrics[app][metric_type]:
                    aggregate_value = (len(self.metrics[app][metric_type][metric])
                                       if metric_type == "PERCENTILE"
                                       else self.metrics[app][metric_type][metric][METRIC_COUNT])
                    all_stats[metric_type][UNIQUE] += 1
                    all_stats[metric_type][AGGREGATE] += aggregate_value
                    app_stats[metric_type][UNIQUE] += 1
                    app_stats[metric_type][AGGREGATE] += aggregate_value
                self.metrics["rumrunner"]["COUNTER"]["rumrunner.{0}.{1}.unique_total".format(app, metric_type)] = [app_stats[metric_type][UNIQUE], 1]
                self.metrics["rumrunner"]["COUNTER"]["rumrunner.{0}.{1}.aggregate_total".format(app, metric_type)] = [app_stats[metric_type][AGGREGATE], 1]
        for metric_type in ("COUNTER", "GAUGE", "PERCENTILE"):
            self.metrics["rumrunner"]["COUNTER"]["rumrunner._all_.{0}.unique_total".format(metric_type)] = [all_stats[metric_type][UNIQUE], 1]
            self.metrics["rumrunner"]["COUNTER"]["rumrunner._all_.{0}.aggregate_total".format(metric_type)] = [all_stats[metric_type][AGGREGATE], 1]

    def collapse_stats(self):
        self.out_metrics = {}
        for app in self.metrics:
            for metric_type in self.metrics[app]:
                for metric in self.metrics[app][metric_type]:
                    val = self.metrics[app][metric_type][metric]
                    if metric_type == "COUNTER":
                        data_points = [val[METRIC_SUM]]
                    elif metric_type == "GAUGE":
                        data_points = [float(val[METRIC_SUM]) / val[METRIC_COUNT]]
                    elif metric_type == "PERCENTILE":
                        data_points = val
                    else:
                        continue  # ignore possible invalid type
                    for dp in data_points:
                        yield [app, metric, metric_type, dp]

    def send_metrics(self):
        """Gather some internal stats on aggregated metrics, and send them one by one for delivery.

        Reset state after delivery.
        """
        self.last_run = time.time()
        self.collect_stats()
        for metric in self.collapse_stats():
            self.send_to_speakeasy(metric)
        self.metrics = {}

    def send_to_speakeasy(self, data):
        "Send a single metric to speakeasy. Don't block as speakeasy server may not be up"
        if self.block:
            self.send_socket.send(json.dumps(data))
        else:
            self.send_socket.send(json.dumps(data), zmq.NOBLOCK)


class MockRumrunner(object):
    def __init__(self, *args, **kwargs):
        pass

    def counter(self, metric_name, value=1):
        pass

    def gauge(self, metric_name, value):
        pass

    def percentile(self, metric_name, value):
        pass

    def send(self, metric_name, value, metric_type):
        pass


def mock_rumrunner():
    Rumrunner.MOCK = True


def unmock_rumrunner():
    Rumrunner.MOCK = False


if __name__ == '__main__':
    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(
        logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S'))
    logging.getLogger().addHandler(stderr_handler)
    logging.getLogger().setLevel(logging.INFO)

    m = Rumrunner('/var/tmp/metric_socket', 'test-app', flush_interval=2)
    s = time.time()
    for x in range(100):
        if x % 10 == 0:
            print(".", sep="", end="")
        m.counter('test_counter', 1)
        m.gauge('test_gauge', x)
        m.percentile('test_percentile.', x)
        time.sleep(0.000001)
    print()
    e = time.time()
    print("Took {0:.3f}s".format(e - s))
    time.sleep(5)
    m.shutdown()
