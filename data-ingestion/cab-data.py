


def read_config():
    """Read config file and return dict."""
    with open('/home/ubuntu/code/.cdp-config.csv') as infile:
        reader = csv.reader(infile)
        config = {row[0]: row[1] for row in reader}
    return config

API endpoint: https://data.cityofchicago.org/resource/wrvz-psew.csv

key name: Python
pp name: PythonCabDownload
