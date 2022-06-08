import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent
README = HERE.joinpath("README.md").read_text()

setup(
    name="route1io-connectors",
    version="0.12.1",
    description="Connectors for interacting with popular API's used in marketing analytics using clean and concise Python code.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/route1io/route1io-python-connectors",
    author="route1.io",
    author_email="developers@route1.io",
    license="GPLv3",
    classifiers=[
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Programming Language :: Python :: 3",
    ],
    packages=find_packages(),
    include_package_data=True,
    install_requires=["boto3", "google-api-python-client", 
                      "google-auth-httplib2", "google-auth-oauthlib",
                      "numpy", "pandas", "requests", "facebook-business", "pyjwt==1.7.1",
                      "aiohttp", "pysftp"],
)

