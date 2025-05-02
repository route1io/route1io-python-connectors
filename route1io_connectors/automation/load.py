import yaml
import os

from route1io_connectors.automation.connectors import s3

LOAD_DISPATCH = {
    "s3": s3.load,
}

def load(config_path="load.yaml"):
    # Load the config
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Get sources
    sources = config['load']['sources']

    # Loop through sources
    for source in sources:
        name = source['name']
        source_type = source['source_type']

        print(f"Starting extraction for source: {name} (type: {source_type})")

        if source_type not in LOAD_DISPATCH:
            raise ValueError(f"No loader defined for source type: {source_type}")

        extractor = LOAD_DISPATCH[source_type]

        # Execute extraction
        data = extractor(source)

        print(f"Load complete for {name}")
