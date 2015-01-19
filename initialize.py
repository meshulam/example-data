import json
import requests
import tempoiq.session
from tempoiq.protocol.device import Device

DEVICE_FILE = "devices.json"
DATA_FILE = "datapoints.json"
CREDENTIALS_FILE = "credentials.json"

def main():
    creds = json.load(open(CREDENTIALS_FILE))
    client = tempoiq.session.get_session(creds['host'],
                                         creds['key'],
                                         creds['secret'])

    client.query(Device).delete()

    devices = json.load(open(DEVICE_FILE))
    for device in devices:
        res = client.create_device(device)
        if res.status == 200:
            print("Created device " + device['key'])
        else:
            print("Error creating device {}: code {}"
                  .format(device['key'], res.status))


if __name__ == "__main__":
    main()
