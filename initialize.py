import re
import sys
import os
import json
import getopt
import requests

DEVICE_FILE = "devices.json"
DATA_FILE = r"datapoints.*\.json"


def delete_everything(creds):
    delete_body = """{"search": {"select": "devices", "filters": {"""
    delete_body += """"devices": "all"}},"find": {"quantifier":"all"}}"""

    device_url = creds['host'] + '/v2/devices'

    res = requests.delete(device_url, data=delete_body,
                          auth=(creds['key'], creds['secret']))
    if (res.status_code != 200):
        print("Delete failed: {} - {}".format(res.status_code, res.text))
        sys.exit(1)


def load_devices_from_file(filename, creds):
    device_url = creds['host'] + '/v2/devices'

    with open(filename) as device_file:
        devices = json.load(device_file)
        for device in devices:
            res = requests.post(device_url, data=json.dumps(device),
                                auth=(creds['key'], creds['secret']))
            if (res.status_code != 200):
                print("Error creating device {}: code {}"
                      .format(device['key'], res.status_code))
                sys.exit(1)


def load_datapoints_from_file(filename, creds):
    datapoint_url = creds['host'] + '/v2/write'

    with open(filename) as point_file:
        payload = ''.join(point_file.readlines())
        res = requests.post(datapoint_url, data=payload,
                            auth=(creds['key'], creds['secret']))
        if (res.status_code != 200):
            print("Data write failed: {} - {}".format(res.status_code, res.text))
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
