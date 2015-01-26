import tempoiq.session
from os import path
import getopt
import sys
import itertools
from tempoiq.protocol.device import Device
from tempoiq.protocol.sensor import Sensor
from tempoiq.protocol.encoder import CreateEncoder
import datetime
import json

DEFAULT_START = datetime.datetime(2000, 1, 1)
DEFAULT_END = datetime.datetime(2016, 1, 1)
DEFAULT_FILTERS = []
# Add device filters like:
#DEFAULT_FILTERS.append(Device.attributes['region'] == 'north')

class Exporter(object):
    def __init__(self, credentials, path='.'):
        cli = tempoiq.session.get_session(credentials['host'],
                                          credentials['key'],
                                          credentials['secret'])
        self.client = cli
        self.devices = []
        self.path = path

    def export_devices(self):
        req = self.client.query(Device)
        for filt in DEFAULT_FILTERS:
            req = req.filter(filt)
        response = req.read()
        for device in response.data:
            self.devices.append(device)

        with open(self.file_in_path('devices.json'), 'w') as outfile:
            json.dump(self.devices, outfile, default=CreateEncoder().default)

    def export_datapoints(self, start, end):
        if not self.devices:
            print("Can't export data without devices!")
            return

        outfile = open(self.file_in_path('datapoints.tsv'), 'w')
        for device in self.devices:
            for (dev, sen, ts, val) in self._read_device(start, end, device):
                outfile.write('{}\t{}\t{}\t{}\n'
                              .format(dev, sen, ts.isoformat(), val))
        outfile.close()

    def file_in_path(self, filename):
        return path.join(self.path, filename)

    def _read_device(self, start, end, device):
        res = self.client.query(Sensor) \
                         .filter(Device.key == device.key) \
                         .read(start=start, end=end)

        for row in res.data:
            for ((device, sensor), value) in row:
                yield (device, sensor, row.timestamp, value)


def main(argv):
    creds = {}
    try:
        opts, args = getopt.getopt(argv, "n:k:s:")
    except getopt.GetoptError:
        print('pull_data.py -n <backend> -k <key> -s <secret>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-k":
            creds['key'] = arg
        elif opt == "-s":
            creds['secret'] = arg
        elif opt == "-n":
            creds['host'] = arg

    if not creds['host'].startswith('http'):
        creds['host'] = 'https://' + creds['host']

    exporter = Exporter(creds)
    exporter.export_devices()
    exporter.export_datapoints(start=DEFAULT_START, end=DEFAULT_END)

if __name__ == "__main__":
    main(sys.argv[1:])