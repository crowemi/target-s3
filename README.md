# target-s3

`target-s3` is a Singer target for s3.

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

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-s3 --about
```

## Settings

| Setting                      | Required | Default | Description |
|:-----------------------------|:--------:|:-------:|:------------|
| aws_access_key               | False    | None    | The aws secret access key for auth to S3. |
| aws_secret_access_key        | False    | None    | The aws secret access key for auth to S3. |
| aws_region                   | True     | None    | The aws region to target |
| bucket                       | True     | None    | The aws bucket to target. |
| prefix                       | False    | None    | The prefix for the key. |
| append_date_to_prefix        | False    | None    | A flag to append the date to the key prefix. |
| append_date_to_prefix_grain  | False    | None    | The grain of the date to append to the prefix. |
| append_date_to_filename      | False    | None    | A flag to append the date to the key filename. |
| append_date_to_filename_grain| False    | None    | The grain of the date to append to the filename. |
| object_format                | False    | None    | The format of the storage object. |
| flatten_records              | False    | None    | A flag indictating to flatten records. |
| stream_maps                  | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config            | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled           | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth         | False    | None    | The max depth to flatten schemas. |



### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your target requires special access on the destination system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `target-s3` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-s3 --version
target-s3 --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-s3 --config /path/to/target-s3-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `target_s3/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-s3` CLI interface directly using `poetry run`:

```bash
poetry run target-s3 --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-s3
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-s3 --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano elt tap-carbon-intensity target-s3
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
