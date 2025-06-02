# Airbus Harvester

The Airbus Harvester is a component of the EODHP (Earth Observation Data Hub Platform) project, designed to regularly collect and process archive imagery metadata from Airbus APIs. This harvester supports both optical (Pléiades, Pléiades Neo, SPOT) and radar (SAR) datasets.

On each run, the harvester queries the relevant Airbus API endpoints, compares the current catalogue with the previous run, and identifies new, updated, or deleted items. It converts the API responses into STAC (SpatioTemporal Asset Catalog) format, storing the resulting STAC items in S3. To track changes efficiently, it maintains a hash of the metadata for each file.

After updating the catalogue, the harvester sends a message to the upstream "harvested" Pulsar topic, enabling downstream components in the EODHP pipeline to react to new or changed data. The harvester also updates the STAC catalogue and collection summaries, including temporal and spatial intervals, to provide a comprehensive and up-to-date view of the available Airbus imagery.

## Features

- Regularly harvests Airbus archive imagery metadata (optical and radar).
- Detects new, updated, and deleted items by comparing metadata hashes with previous runs.
- Converts API responses to STAC-compliant items and collections.
- Stores STAC items in S3.
- Publishes messages to a Pulsar topic for downstream processing.
- Maintains metadata hashes for efficient change tracking.
- Maintains an overarching STAC catalogue and collection.

## Getting Started

### Prerequisites

- Python 3.12+
- GNU Make
- AWS credentials (for S3 access)
- Access to Pulsar (for messaging)
- Access to Airbus APIs

### Setup

Clone the repository and run the setup using the Makefile:

```sh
git clone https://github.com/EO-Datahub/airbus-harvester.git
cd airbus-harvester
make setup
```

This will:

- Create a virtual environment (`venv`)
- Build and install requirements from `pyproject.toml`
- Install pre-commit hooks

You can safely run `make setup` repeatedly; it will only update things if needed.

## Configuration

Configuration is managed via `config.json`.  
You can specify which dataset to harvest by setting the `HARVESTER_CONFIG_KEY` environment variable (e.g., `SPOT`, `PNEO`, `PHR`, `SAR`).

Each dataset configuration in `config.json` controls how the harvester interacts with the corresponding Airbus API. You can adjust:
- **API endpoints and authentication**: Change the `url` and `auth_env` to point to different Airbus API environments or endpoints.
- **Request parameters**: Modify the `body` and `request_method` to control how data is requested (e.g., filtering by constellation, pagination settings).
- **STAC mapping**: Update `stac_properties_map` to map API response fields to STAC properties, or add new mappings as needed.
- **External URLs**: Add or change entries in `external_urls` to include additional links or assets in the output STAC items, and control whether they are proxied.
- **Extensions and metadata**: Specify which STAC extensions to include in the resulting items, and set collection-level metadata.

See `config_schema.json` for config structure.

**Environment Variables:**
- `HARVESTER_CONFIG_KEY`: Selects the dataset config.
- `AIRBUS_API_KEY`: Your Airbus API key.
- `PULSAR_URL`: Pulsar broker URL.
- `PROXY_BASE_URL`: Base URL for asset href redirects via a proxy.
- `MINIMUM_MESSAGE_ENTRIES`: Minimum number of entries before sending a message (default: 100).
- `MAX_API_RETRIES`: Maximum API retry attempts (default: 5).
- `COMMERCIAL_CATALOGUE_ROOT`: Root path for catalogue storage (default: "commercial").
- `TOPIC`: Optional append to the Pulsar output topic, used to separate large harvests such as this from more time-sensitive messages (default: None).

## Usage

Run the harvester from the command line:

```sh
python -m airbus_harvester <workspace_name> <catalog> <s3_bucket>
```

Example:

```sh
python -m airbus_harvester default_workspace catalog catalogue-population-eodhp
```

- `catalog` is not used, it is included to preserve structure with other harvesters
- `workspace_name` should be `default_workspace`, to harvest items into a public catalogue in the EODH.

## Development

- Code is in `airbus_harvester`.
- Formatting: [Black](https://black.readthedocs.io/), [Ruff](https://docs.astral.sh/ruff/), [isort](https://pycqa.github.io/isort/).
- Linting: [Pylint](https://pylint.pycqa.org/).
- Pre-commit checks are installed with `make setup`.

Useful Makefile targets:

- `make test`: Run tests continuously
- `make testonce`: Run tests once
- `make lint`: Lint and reformat code
- `make dockerbuild`: Build a Docker image
- `make dockerpush`: Push a Docker image


## Testing

Run all tests with:

```sh
make testonce
```

Tests use [pytest](https://docs.pytest.org/), [moto](https://github.com/spulec/moto) for AWS mocking, and [requests-mock](https://requests-mock.readthedocs.io/).

## Troubleshooting

- **Authentication errors:** Check your `AIRBUS_API_KEY` and AWS credentials.
- **Pulsar connection issues:** Ensure `PULSAR_URL` is set and reachable.
- **S3 upload failures:** Verify bucket permissions and region.
- **API rate limits:** Adjust `MAX_API_RETRIES` as needed.

Check logs for detailed error messages.


## Release Process

The release process is fully automated and handled through GitHub Actions.  
On every push to `main` or when a new tag is created, the following checks and steps are run automatically:

- Pre-commit checks and linting
- Security scanning
- Unit tests
- Docker image build and push to the configured registry

Versioned releases are handled through the Releases page in github.

See [`.github/workflows/actions.yaml`](.github/workflows/actions.yaml) for details.

## License

This project is licensed under the United Kingdom Research and Innovation BSD Licence. See LICENSE for details.
