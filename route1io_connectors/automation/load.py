import yaml

from route1io_connectors.automation.connectors import s3

LOAD_DISPATCH = {
    "s3": s3.load,
}

def load(config_path="load.yaml"):
    # Load the config
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Get sources
    targets = config['load']['targets']

    # Loop through sources
    for target in targets:
        name = target['name']
        target_type = target['target_type']

        print(f"Starting load to target: {name} (type: {target_type})")

        if target_type not in LOAD_DISPATCH:
            raise ValueError(f"No loader defined for source type: {target_type}")

        extractor = LOAD_DISPATCH[target_type]

        # Execute extraction
        data = extractor(target)

        print(f"Load complete for {name}")
