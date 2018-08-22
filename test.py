#!/usr/bin/env python
# -*- coding:utf-8 -*-

import unittest
import random
import os
import logging
import tempfile
import shutil
import time
import collections

try:
    import ujson as json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        import json

import zmq

from rumrunner import Rumrunner

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] (%(funcName)s) %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


class TestRumrunner(unittest.TestCase):
    def tearDown(self):
        if hasattr(self, "cleanup"):
            self.cleanup()

    def test_send_counter_metric(self):
        def cleanup():
            if rr:
                rr.shutdown()
            recv_socket.close()
            if os.path.exists(tmp_metric_socket):
                os.remove(tmp_metric_socket)
        ctx = zmq.Context()
        recv_socket = ctx.socket(zmq.PULL)
        tmp_metric_socket = '/var/tmp/test_metric_1_{0}'.format(random.random())
        recv_socket.bind('ipc://{0}'.format(tmp_metric_socket))
        self.cleanup = cleanup

        rr = Rumrunner(tmp_metric_socket, 'test_app', start_agent=False, flush_interval=1, agent_socket="inproc://test_send_counter_metric")
        empty = recv_socket.recv()  # suck out empty string for write test
        self.assertEqual(empty, "", "Rumrunner ping should return empty message")
        rr.counter('test_counter')
        rr.agent._run_once()
        self.assertGreater(recv_socket.poll(10), 0, "There should be at least one event received pretty quickly")
        self.assertEqual(recv_socket.recv(), json.dumps(["test_app", "test_counter", "COUNTER", 1]))

    def test_error_out_on_not_writable_socket_disable(self):
        def cleanup():
            if os.path.exists(tmp_metric_socket):
                os.chmod(tmp_metric_socket, 0644)
                os.remove(tmp_metric_socket)

        ctx = zmq.Context()
        recv_socket = ctx.socket(zmq.PULL)
        tmp_metric_socket = '/var/tmp/test_metric_{0}'.format(random.random())
        recv_socket.bind('ipc://{0}'.format(tmp_metric_socket))
        self.cleanup = cleanup

        Rumrunner(tmp_metric_socket, 'test_app', strict_check_socket=False, start_agent=False)
        os.chmod(tmp_metric_socket, 0444)

        # Should not raise an exception due to permissions
        Rumrunner(tmp_metric_socket, 'test_app', strict_check_socket=False, start_agent=False)

    def test_error_out_on_not_writable_socket(self):
        self.skipTest("This test is not reliable - socket permissions are often not respected by OS")
        ctx = zmq.Context()
        recv_socket = ctx.socket(zmq.PULL)
        tmp_metric_socket = '/var/tmp/test_metric_{0}'.format(random.random())
        recv_socket.bind('ipc://{0}'.format(tmp_metric_socket))

        Rumrunner(tmp_metric_socket, 'test_app')

        os.chmod(tmp_metric_socket, 0444)
        self.assertRaises(Exception, Rumrunner, tmp_metric_socket, 'test_app')
        os.remove(tmp_metric_socket)

    def test_mock_rumrunner(self):
        def cleanup():
            if rr:
                rr.shutdown()
        from rumrunner import unmock_rumrunner, mock_rumrunner, MockRumrunner
        rr = Rumrunner('/var/tmp/test', 'test_app', strict_check_socket=False, start_agent=False)
        self.assertTrue(isinstance(rr, Rumrunner))

        mock_rumrunner()
        rr = Rumrunner('/var/tmp/test', 'test_app', strict_check_socket=False, start_agent=False)
        self.assertTrue(isinstance(rr, MockRumrunner))

        unmock_rumrunner()
        rr = Rumrunner('/var/tmp/test', 'test_app', strict_check_socket=False, start_agent=False)
        self.assertTrue(isinstance(rr, Rumrunner))

    def test_rumrunner_agent(self):
        tdir = tempfile.mkdtemp()
        rr1 = None
        rr2 = None

        def cleanup():
            shutil.rmtree(tdir)
            if rr1:
                rr1.shutdown()
            if rr2:
                rr2.shutdown()
        self.cleanup = cleanup

        tmp_metric_socket = os.path.join(tdir, 'test_metric_{0}'.format(random.random()))
        # logging.info("Using socket file %s", tmp_metric_socket)
        ctx = zmq.Context.instance()
        recv_socket = ctx.socket(zmq.PULL)
        recv_socket.bind('ipc://{0}'.format(tmp_metric_socket))
        rr1 = Rumrunner(tmp_metric_socket, "appA", start_agent=False, flush_interval=2)  # this will bind but not start the agent thread
        rr2 = Rumrunner(tmp_metric_socket, "appA")  # let us imagine another thread in the same process creates another instance
        self.assertIsNotNone(rr1.agent, "The first rumrunner instance should get the socket")
        self.assertIsNone(rr2.agent, "The second rumrunner instance should NOT get the socket")

        for x in range(10):
            # 10 counters from each thread should aggregate to 20
            rr1.counter("aCounter")
            rr2.counter("aCounter")
            # 10 gauges from each thread should average to (10+15)*10/20=12.5
            rr1.gauge("bGauge", 10)
            rr2.gauge("bGauge", 15)
            # percentiles should remain as such, [10, 15, ...], length being 20 elements
            rr1.percentile("cPercentile", 10)
            rr2.percentile("cPercentile", 15)

        events = {}
        event_count = [0]

        def capture_event(data):
            if data[0] != "rumrunner":
                app, metric, mtype, val = data
                if app not in events:
                    events[app] = collections.defaultdict(list)
                events[app][metric].append({"type": mtype, "value": val})
                event_count[0] += 1  # workaround to get at the no-global variable just outside the scope

        # override the  original method to capture the events sent
        rr1.agent.send_to_speakeasy = capture_event

        # run one collection loop of flush_interval seconds
        rr1.agent.last_run = init_time = time.time() - 1
        rr1.agent._run_once()
        rr1.shutdown()
        rr1.shutdown()
        self.assertGreater(rr1.agent.last_run, init_time,
                           "After a collection loop, the last run time should be updated")

        # while True:
        #     num_events = recv_socket.poll(500)
        #     if num_events > 0:
        #         data = recv_socket.recv()
        #         if not len(data):
        #             continue
        #         events.append(json.loads(data))
        #     else:
        #         break
        # print ">>>>>>>>>", events
        self.assertEqual(event_count[0], 22,
                         "Should return one data point each for gauge/counter and all the ones for percentile")
        self.assertEqual(events["appA"]["aCounter"][0]["value"], 20)
        self.assertEqual(events["appA"]["bGauge"][0]["value"], 12.5)
        self.assertEqual(len(events["appA"]["cPercentile"]), 20)


if __name__ == '__main__':
        unittest.main()
