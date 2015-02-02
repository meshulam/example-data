import gevent.monkey
gevent.monkey.patch_all()  # Do this before other imports

import gevent
from gevent.queue import JoinableQueue

from tempoiq.protocol.encoder import WriteEncoder, CreateEncoder
from tempoiq.session import get_session
import tempoiq.response
import re
import sys
import os
import json
import getopt
import requests

DEVICE_FILE = r"devices\.json"
DATA_FILE = r"datapoints[0-9]*\.tsv"
DATA_PATH = '.'

class Importer(object):
    def __init__(self, creds, pool_size=5):
        self.client = get_session(creds['host'],
                                  creds['key'],
                                  creds['secret'])
        self.queue = JoinableQueue()
        for i in range(pool_size):
            gevent.spawn(self.worker)

    def worker(self):
        while True:
            job = self.queue.get()
            typ = job.get('type')
            try:
                if typ == 'device':
                    self._process_device(job['data'])
                elif typ == 'datapoints':
                    self._process_datapoints(job['data'])
            finally:
                self.queue.task_done()

    def write_devices(self, devices):
        for device in devices:
            self.queue.put({'type': 'device', 'data': device})
        self.queue.join()

    def write_datapoints_from_file(self, infile):
        points = {}
        lineno = 0
        for line in infile:
            lineno += 1
            (device, sensor, ts, val) = line.split('\t')
            pts = points.setdefault(device, {}).setdefault(sensor, [])
            pts.append({'t': ts, 'v': float(val)})

            if lineno % 1000 == 0:
                self.queue.put({'type': 'datapoints', 'data': points})
                points = {}

        if points:
            self.queue.put({'type': 'datapoints', 'data': points})
        self.queue.join()

    def _process_device(self, device, retries=5):
        res = self.client.create_device(device)
        if res.successful != tempoiq.response.SUCCESS:
            if 'A device with that key already exists' in res.body:
                print("Skipping creating existing device {}"
                      .format(device['key']))
                return

            if retries > 0:
                print("Retrying device create {}, error {}"
                      .format(device['key'], res.body))
                self._process_device(device, retries - 1)
            else:
                print("Retries exceeded; couldn't create device {}"
                      .format(device['key']))

    def _process_datapoints(self, write_request, retries=5):
        try:
            res = self.client.write(write_request)
        except Exception, e:
            print("ERROR with request: --->")
            print(json.dumps(write_request, default=WriteEncoder().default))
            raise e

        if res.successful != tempoiq.response.SUCCESS:
            if retries > 0:
                print("Retrying write, error was: {}".format(res.body))
                return self._process_datapoints(write_request, retries - 1)
            else:
                print("Retries exceeded; lost data!")
                print(json.dumps(write_request, default=WriteEncoder().default))
                return True
        return False

def parse_credentials(argv):
    creds = {}
    try:
        opts, args = getopt.getopt(argv, "n:k:s:")
    except getopt.GetoptError:
        print('init_data.py -n <backend> -k <key> -s <secret>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-k":
            creds['key'] = arg
        elif opt == "-s":
            creds['secret'] = arg
        elif opt == "-n":
            creds['host'] = arg

    if 'key' in creds and 'secret' in creds and 'host' in creds:
        if not creds['host'].startswith('http'):
            creds['host'] = 'https://' + creds['host']
        return creds
    else:
        return None

def main(argv):
    creds = parse_credentials(argv)
    if not creds:
        print("Invalid credentials!")
        sys.exit(2)

    importer = Importer(creds)

    files = os.listdir(DATA_PATH)
    device_files = []
    data_files = []
    for f in files:
        if re.match(DATA_FILE, f):
            data_files.append(os.path.join(DATA_PATH, f))
        if re.match(DEVICE_FILE, f):
            device_files.append(os.path.join(DATA_PATH, f))

    for filename in device_files:
        with open(filename) as device_file:
            devices = json.load(device_file)
            importer.write_devices(devices)

    for filename in data_files:
        with open(filename) as point_file:
            importer.write_datapoints_from_file(point_file)

if __name__ == "__main__":
    main(sys.argv[1:])
