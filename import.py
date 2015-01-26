import re
import sys
import os
import json
import getopt
import requests

DEVICE_FILE = r"devices\.json"
DATA_FILE = r"datapoints[0-9]*\.tsv"
DATA_PATH = '.'


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
                if 'A device with that key already exists' in res.text:
                    print("Skipping creating existing device {}"
                          .format(device['key']))
                else:
                    print("Error creating device {}: code {}"
                          .format(device['key'], res.status_code))
                    sys.exit(1)

def write_points(points, creds):
    data_url = creds['host'] + '/v2/write/'
    res = requests.post(data_url, data=json.dumps(points),
                        auth=(creds['key'], creds['secret']))

    if (res.status_code != 200):
        print("Error writing data! code {}"
              .format(res.status_code))
        sys.exit(1)

def load_datapoints_from_file(filename, creds):
    points = {}
    lineno = 0
    with open(filename) as point_file:
        for line in point_file:
            lineno += 1
            (device, sensor, ts, val) = line.split('\t')
            pts = points.setdefault(device, {}).setdefault(sensor, [])
            pts.append({'t': ts, 'v': float(val)})

            if lineno % 1000 == 0:
                write_points(points, creds)
                points = {}

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

    files = os.listdir(DATA_PATH)
    data_files = []
    for f in files:
        if re.match(DATA_FILE, f):
            data_files.append(os.path.join(DATA_PATH, f))
        if re.match(DEVICE_FILE, f):
            load_devices_from_file(os.path.join(DATA_PATH, f), creds)

    for data_file in data_files:
        load_datapoints_from_file(data_file, creds)

if __name__ == "__main__":
    main(sys.argv[1:])
