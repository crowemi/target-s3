# target-s3

`target-s3` is inteded to be a multi-format/multi-cloud Singer target.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPi:

```bash
pipx install target-s3
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/target-s3.git@main
```

-->

## Configuration

### Accepted Config Options

```json
{
    "format": {
        "format_type": "json",
        "format_parquet": {},
        "format_json": {},
        "format_csv": {}
    },
    "cloud_provider": {
        "cloud_provider_type": "aws",
        "aws": {
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "aws_region": "us-west-2",
            "aws_profile_name": "test-profile",
            "aws_bucket": "test-bucket",
            "aws_endpoint_override": "http://localhost:4566"
        }
    },
    "prefix": "path/to/output",
    "stream_name_path_override": "StreamName",
    "include_process_date": true,
    "append_date_to_prefix": false,
    "append_date_to_prefix_grain": "day",
    "append_date_to_filename": true,
    "append_date_to_filename_grain": "microsecond",
    "flatten_records": false
}
```


<!--
Developer TODO: Provide a list of config options accepted by the target.

This section can be created by copy-pasting the CLI output from:

```
target-s3 --about --format=markdown
```
-->
## Capabilities

* `about`
* `stream-maps`
* `schema-flattening`

## Settings

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

## Usage

You can easily run `target-s3` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-s3 --version
target-s3 --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-s3 --config /path/to/target-s3-config.json
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
