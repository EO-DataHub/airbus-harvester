# Changelog

# 0.1.9
- Improved reliability and logging

# 0.1.8
- Support environment variable to specify TOPIC extensions, to allow separate Pulsar topics for bulk harvesting
- Support optical thumbnail proxy

# 0.1.7
- Updated code to use Messager Framework defined in eodhp-utils
- Updated tests to use same framework
- Remove Pulsar dependency in tests

# 0.1.6
- Added retries and updated producer name

# 0.1.5
- Added configuration for optical data harvesters

# 0.1.4
- Simplified GET request

# 0.1.3
- Update requirements

# 0.1.2
- Harvest whole catalogue

# 0.1.1 (2024/09/02)
- Added thumbnails and metadata in line with STAC conventions
- Added unit tests

# 0.1.0 (2024/08/09)
- Collects Airbus data, saves it to S3 bucket and sends message via pulsar

