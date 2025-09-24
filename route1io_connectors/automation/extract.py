import yaml

from route1io_connectors.automation.connectors import s3, gsheets

EXTRACTION_DISPATCH = {
    "s3": s3.extract,
    "gsheets": gsheets.extract
}

def extract(config_path="extraction.yaml"):
    # Load the config
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Get sources
    sources = config['extract']['sources']

    # Loop through sources
    for source in sources:
        name = source['name']
        source_type = source['source_type']

        print(f"Starting extraction for source: {name} (type: {source_type})")

        if source_type not in EXTRACTION_DISPATCH:
            raise ValueError(f"No extractor defined for source type: {source_type}")

        extractor = EXTRACTION_DISPATCH[source_type]

        # Execute extraction
        data = extractor(source)

        print(f"Extraction complete for {name}")
