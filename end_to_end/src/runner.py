import os
import sys
import json
import socket
import time
import select
import threading
import nsq
import tornado.ioloop
from service.db import DB
from decouple import config
from helper import hex_to_bytes, bytes_to_str, split_hex, load_payload
from datetime import datetime, timezone


class RunTest:
    def __init__(self):
        self._db = DB()
        self.error_test_case = []

        self.run_nsq = False
        self.nsq_thread = None
        self.nsq_loop = None
        self.nsq_reader = None
        self.nsq_messages = []
        self.nsq_lock = threading.Lock()
        self.nsq_started = threading.Event()

        self.block_event = threading.Event()
        self.block_event.set()

    def _start_nsq(self, topic='PACKET'):
        if self.nsq_thread and self.nsq_thread.is_alive():
            return

        def _nsq_thread_fn(topic_name):
            loop = tornado.ioloop.IOLoop()
            tornado.ioloop.IOLoop.clear_instance()
            loop.make_current()
            self.nsq_loop = loop

            channel_name = f"test_runner_{int(time.time())}_{threading.get_ident()}"
            self.nsq_reader = nsq.Reader(
                message_handler=self._nsq_msg_handler,
                nsqd_tcp_addresses=['127.0.0.1:4150'],
                topic=topic_name,
                channel=channel_name,
                lookupd_poll_interval=1
            )

            self.nsq_started.set()

            try:
                loop.start()
            finally:
                try:
                    self.nsq_reader.close()
                except Exception:
                    pass

        t = threading.Thread(target=_nsq_thread_fn, args=(topic,), daemon=True)
        t.start()
        self.nsq_thread = t

        if not self.nsq_started.wait(timeout=5):
            raise RuntimeError("Failed to start NSQ reader within timeout")

    def _nsq_msg_handler(self, message):
        with self.nsq_lock:
            self.nsq_messages.append(message.body)
            # print(message.body)
        return True

    def _nsq_checker(self, expected: dict, timeout):
        start = time.time()

        while time.time() - start < timeout:
            for idx, msg_raw in enumerate(self.nsq_messages):
                try:
                    msg = json.loads(msg_raw.decode() if isinstance(msg_raw, bytes) else msg_raw)
                except json.JSONDecodeError:
                    print('[ERROR] Failed decode message nsq')
                    continue

                msg_obj = list(msg.keys())
                exp_obj = list(expected.keys())

                if set(msg_obj) != set(exp_obj):
                    continue

                matched = True

                for k in exp_obj:
                    if isinstance(expected[k], dict):
                        try:
                            message = msg[k] if isinstance(msg[k], dict) else json.loads(msg[k])
                        except:
                            matched = False
                            break

                        # recursive check for nested dicts like "message"
                        for sub_k, sub_v in expected[k].items():
                            if sub_v != '' and str(message.get(sub_k)) != str(sub_v):
                                matched = False
                                break
                    else:
                        if expected[k] != '' and str(msg.get(k)) != str(expected[k]):
                            matched = False
                            break
                        else:
                            continue

                if matched:
                    del self.nsq_messages[idx]
                    if 'data' in msg and len(self.nsq_messages) > 0:
                        for x, msg_ in enumerate(self.nsq_messages):
                            msg_ = json.loads(msg_.decode() if isinstance(msg_, bytes) else msg_)
                            if 'data' in msg_ and msg_['data'] == msg['data']:
                                print('Duplicate data', msg['data'])
                                del self.nsq_messages[x]
                    return True

            time.sleep(0.1)

        return False

    def run(self, files=None):
        folder = 'src/payloads'

        if not os.path.isdir(folder):
            print(f"[ERROR] Folder does not exist: {folder}")
            return

        if isinstance(files, str):
            files = [files]
        elif files is not None:
            files = list(files)

        for file_name in os.listdir(folder):
            if files and file_name not in files:
                continue

            file_path = os.path.join(folder, file_name)
            print(f"\n[INFO] Running test case: {file_name}")
            try:
                self.run_test_tcp(file_path)
            except FileNotFoundError:
                print(f"[ERROR] File not found: {file_path}")
            except json.JSONDecodeError as e:
                print(f"[ERROR] Invalid JSON in {file_name}: {e}")

    def run_test_tcp(self, filename: str):
        test_case = load_payload(filename)
        msg_type = test_case.get('message_type', 'hex')
        device_type = test_case.get('device_type')

        self.run_nsq = False
        # Start NSQ consumer if needed
        if test_case.get('nsq_check'):
            try:
                self._start_nsq(topic='PACKET')
                self.run_nsq = True
            except Exception as e:
                print(f"[ERROR] Starting NSQ reader failed: {e}")

        host = config('TCP_HOST', default='localhost')
        port = int(config('TCP_PORT', default=1200))
        for case in test_case.get('test_case', []):
            connections = case.get('connections')
            if not connections:
                print(f"[WARN] No 'connection' section in case: {case.get('name')}")
                continue

            # print(f"\n[INFO] Running case '{case['name']}' with {len(connections)} connections")

            threads = []
            self.result_lock = threading.Lock()

            for idx, conn in enumerate(connections, start=1):
                delay = 0

                for x in conn['steps']:
                    host = x['pod_ip'] if 'pod_ip' in x else host
                    port = int(x['port']) if 'port' in x else port
                    delay = x['delay'] if 'delay' in x else delay
                    if 'name' not in x:
                        continue

                time.sleep(delay)
                t = threading.Thread(
                    target=self._run_connection_steps,
                    args=(idx, host, port, conn['steps'], msg_type, device_type, case['name'])
                )
                t.start()
                threads.append(t)

            # Wait for all to finish
            for t in threads:
                t.join()

            # print(f"[INFO] Completed test case '{case['name']}'")
        print('\nFAILED: ', self.error_test_case)

    def _run_connection_steps(self, conn_id: int, host, port, steps, msg_type: str, device_type: str, case_name: str):
        self.block_event.wait()
        result = {}
        try:
            sock = socket.socket()
            sock.connect((host, port))
            sock.settimeout(30)
            # print(f"[INFO] Conn-{conn_id}: Connected")

            for step in steps:
                step: dict
                if step.get('block', False):
                    # Pause all other connection threads until this step finishes.
                    self.block_event.clear()
                else:
                    # Ensure the block event is released so other threads can continue.
                    self.block_event.set()

                step_name = step.get('name')
                if not step_name:
                    continue
                step_result = {
                    "status": "pass",
                    "notes": "",
                    "conn": True
                }

                try:
                    if msg_type == 'string' or step['send'][:4] == '8080':
                        msg = step['send'].encode()
                    elif msg_type == 'hex':
                        msg = hex_to_bytes(step['send'])
                    else:
                        raise ValueError(f"Unsupported message_type: {msg_type}")

                    # Run pre_test SQL if any
                    if pre_test := step.get('pre_test'):
                        for query in pre_test:
                            self._db.save(query)
                        time.sleep(2)

                    sock.sendall(msg)
                    # print(f"[DEBUG] Conn-{conn_id} → sent {step_name}")

                    # ACK checker
                    buffer = []
                    inactivity_timeout = 1
                    poll_interval = 0.1
                    last_received = time.time()
                    sock.setblocking(False)

                    while True:
                        ready, _, _ = select.select([sock], [], [], poll_interval)
                        if ready:
                            data = sock.recv(4096)
                            if data == b'':
                                if step == steps[-1]:
                                    # delay, waiting db and nsq update
                                    time.sleep(2)
                                else:
                                    step_result['conn'] = False
                                    step_result['notes'] += ', connection closed by server'
                                break
                            buffer.append(data)
                            last_received = time.time()
                        if time.time() - last_received > inactivity_timeout:
                            break

                    # decode ACKs
                    if msg_type == 'hex':
                        ack = [msg for x in buffer for msg in split_hex(device_type, bytes_to_str(x))]
                    else:
                        ack = buffer[0].decode() if buffer else ""

                    # Verify ACK
                    expected_ack = step.get('expect_ack', [])
                    if expected_ack and ack != expected_ack:
                        step_result['ack'] = False
                        step_result['notes'] += f', ack expected {expected_ack}, got {ack}'
                        # raise AssertionError(f"ACK mismatch — expected {expected_ack}, got {ack}")
                    else:
                        step_result["ack"] = True

                    if step == steps[-1] and step_result['conn']:
                        sock.close()
                        step_result['conn'] = True
                        # delay, waiting db and nsq update
                        time.sleep(2)

                    # DB CHECK
                    if 'db_check' not in step:
                        step_result["db"] = "skipped"
                    else:
                        db_pass = True
                        for db in step['db_check']:
                            db:dict
                            time.sleep(db.get('delay', 2))

                            query = db.get('query')
                            if not query:
                                continue

                            result = self._db.fetch_one_dict(query, db.get('params', []))
                            assertions = db.get('assertions', {})

                            if not result:
                                if not assertions:
                                    step_result["db"] = "pass"
                                    continue

                            for k, expected_value in db.get('assertions', {}).items():
                                actual_value = result.get(k)

                                # ignore if expected is empty string
                                if expected_value == '':
                                    continue

                                if isinstance(actual_value, datetime):
                                    expected_dt = datetime.fromisoformat(str(expected_value)).replace(tzinfo=timezone.utc)
                                    if abs((actual_value - expected_dt).total_seconds()) > 300:
                                        db_pass = False
                                        step_result['notes'] += f', db mismatch [{k}] — expected {expected_value}, got {actual_value}'
                                        break
                                elif str(actual_value) != str(expected_value):
                                    db_pass = False
                                    step_result['notes'] += f', db mismatch [{k}] — expected {expected_value}, got {actual_value}'
                                    break

                        step_result["db"] = db_pass

                    # NSQ CHECK
                    if self.run_nsq and 'nsq_check' in step:
                        nsq_exp = step['nsq_check']

                        if len(nsq_exp) == 0:
                            if len(self.nsq_messages) > 0:
                                raise AssertionError(f"Unexpected NSQ messages: {self.nsq_messages}")
                            step_result["nsq"] = "skipped"
                        else:
                            for expected in nsq_exp:
                                can_be_failed = expected.pop("expected_failed", False)
                                # print(expected)
                                timeout = 10 if not can_be_failed else 3
                                if not self._nsq_checker(expected, timeout):
                                    step_result["nsq"] = can_be_failed
                                    step_result['notes'] += f', nsq msg not found expected: {expected}'
                            if len(self.nsq_messages) == 0:
                                step_result["nsq"] = True
                            elif len(self.nsq_messages) != 0:
                                step_result["nsq"] = can_be_failed
                                step_result['notes'] += f', nsq msg found {self.nsq_messages}'
                    else:
                        step_result['nsq'] = 'skipped'

                except Exception as e:
                    step_result["status"] = "FAILED!"
                    step_result["error"] = str(e)
                    # print(f"[ERROR] Conn-{conn_id} {step_name}: {e}")

                # process result
                result = {
                    'status': 'PASS',
                    'ack': step_result.get('ack', False),
                    'db': step_result.get('db', False),
                    'nsq': step_result.get('nsq', False),
                    'conn': step_result.get('conn', False),
                    'step': f'{case_name}, {step_name}'
                }

                if step_result.get('notes'):
                    result['notes'] = step_result['notes']
                if step_result.get('error'):
                    result['error'] = step_result['error']

                if any(not v for k, v in result.items() if k not in ('step', 'status')):
                    result['status'] = 'FAIL'

                print(result)
                if result['nsq'] != 'skipped' and len(self.nsq_messages) > 0:
                    print(f'nsq message: ', self.nsq_messages)

                if result['status'] == 'FAIL':
                    self.error_test_case.append(result['step'])

            # print(f"[INFO] Conn-{conn_id}: Completed all steps.")

        except Exception as e:
            print(f"[ERROR] Conn-{conn_id}: connection failed — {e}")

if __name__ == "__main__":
    running = RunTest()
    if len(sys.argv) > 1:
        running.run(files=sys.argv[1])
    else:
        running.run()

# python3 src/runner.py test_case_concox.yml