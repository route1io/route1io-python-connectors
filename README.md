# route1.io Python API connectors

[![Issues](https://img.shields.io/github/issues/route1io/route1io-python-connectors)](https://github.com/route1io/route1io-python-connectors/issues)
[![License](https://img.shields.io/github/license/route1io/route1io-python-connectors)](https://www.gnu.org/licenses/gpl-3.0.en.html)
[![Version](https://img.shields.io/pypi/v/route1io-connectors?color=brightgreen)](https://pypi.org/project/route1io-connectors/)

---

At [route1.io](http://route1.io/index.html) we've written high level, easy-to-use abstractions in Python for connecting to common APIs used in marketing analytics. This repository contains our official code, documentation, and tutorials to help you on your way accessing what matters most: *your data*.

![Sample gif showing importing the connectors](media/route1io_import.gif)

## Table of Contents
* [Supported API's](#supported)
* [Installation](#installation)
* [Sample usage](#usage)
* [License](#license)

---

## Supported API's <a name="supported"></a>
Below is a list of API's currently supported by our custom connectors
* [AWS' S3](route1io_connectors/aws.py)
* [Google Sheets](route1io_connectors/gsheets.py)
* [Google Analytics](route1io_connectors/google_analytics.py)
* [Google Ads](route1io_connectors/googleads.py)
* [DoubleClick Manager](route1io_connectors/dcm.py)
* [Search Ads 360](route1io_connectors/sa360.py)
* [Slack](route1io_connectors/slack.py)
* [Facebook](route1io_connectors/facebook.py)
* [Apple Search Ads](route1io_connectors/apple_search_ads.py)
* [TikTok](route1io_connectors/tiktok.py)
* [OneDrive](route1io_connectors/onedrive.py)

---

## Installation <a name="installation"></a>

### pip
Easily install to your environment from PyPI with 
```shell
$ pip install route1io-connectors
```

---

## Sample usage <a name="usage"></a>
All connectors provided by the package can be imported with Python's standard import syntax. See our [wiki](https://github.com/route1io/route1io-python-connectors/wiki) for tutorials regarding specific platform connectors!


```python3
from route1io_connectors import aws

# Credentials 
# NOTE: Storing credentials in source isn't secure in practice. 
#       It's recommended you store them in a more secure place.
AWS_ACCESS_KEY_ID = "YOUR_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "YOUR_AWS_SECRET_ACCESS_KEY"
REGION_NAME = "YOUR_DEFAULT_REGION_NAME"

# Connect to S3 
s3 = aws.connect_to_S3(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION_NAME
)

# Download remote file from S3 bucket to local machine
aws.download_from_S3(
    s3=s3,
    bucket="your.s3.bucket",
    key="local_file.csv"
    local_fpath="tmp/local_file.csv",
)
```

<!-- ---

## Documentation <a name="documentation"></a>
The official documentation can be found [here](docs/_build/html/index.html). -->

---

## License <a name="license"></a>
This library operates under the [GNU GPL v3.0](LICENSE) license.

---

<p align="center">
  <img src="media/route1io.png" width="50px">
</p>
