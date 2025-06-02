# Changelog

# 0.1.18 (2025/05/29)
- Remove extra `Z`s from temporal intervals

# 0.1.17 (2025/05/29)
- Remove extra `catalogs` from proxied thumbnail URLs

# 0.1.16 (2025/04/29)
- Update metadata file generation

# 0.1.15 (2025/04/04)
- Hotfix for recent geometry_centroid data

# 0.1.14 (2025/04/04)
- Standardise geometry_centroid

# 0.1.13 (2025/03/28)
- Update descriptions; add thumbnail links
- Add providers to collections
- Update paths

# 0.1.12 (2025/03/10)
- Read Catalog Root from Environment Variable
  - Set default to be commercial catalog for airbus data
  - Also update logging level to print INFO statements

# 0.1.11 (2025/02/18)
- Item assets only need to be linked under assets not links
  - Remove function to add assets to both links and asset objects, now only added to assets
  - This caused issues with the link types when ingesting with STAC-FastApi

# 0.1.10 (2025/02/10)
- Update actions

# 0.1.9 (2025/02/04)
- Improved reliability and logging

# 0.1.8 (2025/01/29)
- Support environment variable to specify TOPIC extensions, to allow separate Pulsar topics for bulk harvesting
- Support optical thumbnail proxy

# 0.1.7 (2024/12/16)
- Updated code to use Messager Framework defined in eodhp-utils
- Updated tests to use same framework
- Remove Pulsar dependency in tests

# 0.1.6 (2024/12/10)
- Added retries and updated producer name

# 0.1.5 (2024/12/10)
- Added configuration for optical data harvesters

# 0.1.4 (2024/11/11)
- Simplified GET request

# 0.1.3 (2024/11/01)
- Update requirements

# 0.1.2 (2024/10/25)
- Harvest whole catalogue

# 0.1.1 (2024/09/02)
- Added thumbnails and metadata in line with STAC conventions
- Added unit tests

# 0.1.0 (2024/08/09)
- Collects Airbus data, saves it to S3 bucket and sends message via pulsar

