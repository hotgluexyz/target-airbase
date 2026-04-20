# target-airbase

`target-airbase` is a Singer target for [Airbase](https://www.airbase.com/) accounting data, built with the Hotglue Singer SDK. It loads chart-of-accounts, vendors, subsidiaries, and currencies into the Airbase accounting API and coordinates an entity sync session around each job run.

## Overview

- **Entity sync session**: Before reading Singer input, the target sends `POST {base}/entity/sync/` with `{"action": "START"}`. After all input is processed and sinks are drained, it sends `PATCH` to the same path with `{"action": "COMPLETE"}`.
- **Streams**: Each supported tap stream maps to a dedicated sink. Records are validated against [hotglue-models-accounting](https://gitlab.com/hotglue/hotglue-models-accounting) schemas where applicable, transformed in `preprocess_record`, then written with `POST` (create) or `PATCH` (update when the record carries an `id` from a prior run).
- **Reference data**: For streams that need subsidiary or currency resolution, the target loads `/subsidiaries` and `/currencies` once per job (cached on the target instance) and matches ERP identifiers to Airbase payloads.

### Entity sync edge cases

If Airbase reports that general-ledger onboarding is already finished, the helpers avoid failing the job:

- **START**: HTTP `409` is treated as success (no further action).
- **COMPLETE**: HTTP `404` with detail `No active sync session found.` is treated as success (no further action).

## Installation

```bash
pip install .
```

From a clone of this repository:

```bash
cd target-airbase
poetry install
```

`hotglue-models-accounting` is declared as a Git dependency in `pyproject.toml`; ensure your environment can reach that Git host when installing.

## Configuration

### Required settings

| Setting   | Description |
|-----------|-------------|
| `api_key` | Airbase API token (`Authorization: Token …`). |

### Example `config.json`

```json
{
  "api_key": "your-airbase-api-token"
}
```

## API base URL

The accounting client uses a fixed base URL in code (override in `target_airbase/clients.py` if you use another environment):

- **`https://api-stage.airbase.io/v1/accounting`**

Entity sync calls use `{base_url}/entity/sync/` (same host and `/v1/accounting` prefix as other accounting routes).

## Supported streams

Singer **stream** names must match the sink `name` (case-insensitive per SDK). Each sink posts to the path shown.

| Singer stream   | REST path (under base URL) | Notes |
|-----------------|----------------------------|--------|
| `Accounts`      | `/accounts/`               | Maps accounting `Account` fields into Airbase account payloads; resolves subsidiaries and currency by ISO code. |
| `Vendors`       | `/vendors/`                | Maps `Vendor` fields; subsidiary resolution by subsidiary number. |
| `Subsidiaries`  | `/subsidiaries/`           | Requires at least one address with `country` for `iso_code`. |
| `Currencies`    | `/currencies/`             | Pass-through of the record after preprocessing (default: unchanged). |

**Create vs update**: If `preprocess_record` leaves an `id` on the record, the sink sends `PATCH` to `/{endpoint}/{id}`; otherwise it sends `POST` to the stream endpoint with the JSON body.

## Usage

```bash
# Version and help
target-airbase --version
target-airbase --help

# Pipe a tap
tap-your-source | target-airbase --config config.json
```

For local debugging, run the package as a module from the repository root so imports resolve:

```bash
python -m target_airbase.target --config path/to/config.json < tap_output.jsonl
```

## Development

```bash
pipx install poetry   # optional
poetry install
poetry run pytest
poetry run target-airbase --help
```

## License

Apache 2.0
