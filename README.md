# route1io-python-connectors

---

This repository contains [route1.io](http://route1.io/index.html)'s official code, documentation, and tutorials for connecting to popular API's used in marketing analytics with Python.

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
* [Search Ads 360](route1io_connectors/sa360.py)
* [Slack](route1io_connectors/slack.py)
* [Facebook](route1io_connectors/facebook.py)

---

## Installation <a name="installation"></a>

### git
Clone this repo locally with
```shell
$ git clone https://github.com/route1io/route1io-python-connectors.git
```
and then copy *route1io_connectors* into your projects directory

---

## Sample usage <a name="usage"></a>
All connectors provided by the package can be imported with Python's standard import syntax:
```python
from route1io_connectors import aws, sa360, gsheets, slack
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
