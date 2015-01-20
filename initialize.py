import re
import os
import json
import requests
import tempoiq.session
from tempoiq.protocol.device import Device

DEVICE_FILE = "devices.json"
DATA_FILE = r"datapoints.*\.json"
CREDENTIALS_FILE = "credentials.json"


def delete_everything(creds):
    client = tempoiq.session.get_session(creds['host'],
                                         creds['key'],
                                         creds['secret'])
    client.query(Device).delete()


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


def load_datapoints_from_file(filename, creds):
    datapoint_url = creds['host'] + '/v2/write'

    with open(filename) as point_file:
        payload = ''.join(point_file.readlines())
        res = requests.post(datapoint_url, data=payload,
                            auth=(creds['key'], creds['secret']))
        print("Wrote data points: ", res.status_code)

def main():
    creds = json.load(open(CREDENTIALS_FILE))
    delete_everything(creds)

    data_path = os.path.join(os.getcwd(), "data")
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
    main()
