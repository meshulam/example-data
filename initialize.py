import re
import sys
import os
import json
import getopt
import requests
import tempoiq.response
import tempoiq.session
from tempoiq.protocol.device import Device


DEVICE_FILE = "devices.json"
DATA_FILE = r"datapoints.*\.json"


def delete_everything(creds):
    client = tempoiq.session.get_session(creds['host'],
                                         creds['key'],
                                         creds['secret'])
    res = client.query(Device).delete()
    if res.successful != tempoiq.response.SUCCESS:
        sys.exit(1)


def load_devices_from_file(filename, creds):
    client = tempoiq.session.get_session(creds['host'],
                                         creds['key'],
                                         creds['secret'])
    with open(filename) as device_file:
        devices = json.load(device_file)
        for device in devices:
            res = client.create_device(device)
            if res.status == 200:
                print("Created device " + device['key'])
            else:
                print("Error creating device {}: code {}"
                      .format(device['key'], res.status))
                sys.exit(1)


def load_datapoints_from_file(filename, creds):
    datapoint_url = creds['host'] + '/v2/write'

    with open(filename) as point_file:
        payload = ''.join(point_file.readlines())
        res = requests.post(datapoint_url, data=payload,
                            auth=(creds['key'], creds['secret']))
        if (res.status_code != 200):
            sys.exit(1)

def main(argv):
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

    if not creds['host'].startswith('http'):
        creds['host'] = 'https://' + creds['host']

    delete_everything(creds)

    script_path = os.path.dirname(os.path.realpath(sys.argv[0]))
    data_path = os.path.join(script_path, "data")
    files = os.listdir(data_path)
    data_files = []
    for f in files:
        if re.match(DATA_FILE, f):
            data_files.append(os.path.join(data_path, f))
        if f == DEVICE_FILE:
            load_devices_from_file(os.path.join(data_path, f), creds)

    for data_file in data_files:
        load_datapoints_from_file(data_file, creds)


if __name__ == "__main__":
    main(sys.argv[1:])
