# S3 file input plugin for Embulk

## Overview

* Plugin type: **file input**
* Resume supported: **yes**
* Cleanup supported: **yes**

## Configuration

- **bucket** S3 bucket name (string, required)

- **path_prefix** prefix of target keys (string, required)

- **endpoint** S3 endpoint login user name (string, optional)

- **auth_method**: name of mechanism to authenticate requests (basic, env, instance, profile, properties, anonymous, or session. default: basic)

  - "basic": uses access_key_id and secret_access_key to authenticate.

    - **access_key_id**: AWS access key ID (string, required)

    - **secret_access_key**: AWS secret access key (string, required)

  - "env": uses AWS_ACCESS_KEY_ID (or AWS_ACCESS_KEY) and AWS_SECRET_KEY (or AWS_SECRET_ACCESS_KEY) environment variables.

  - "instance": uses EC2 instance profile.

  - "profile": uses credentials written in a file. Format of the file is as following, where `[...]` is a name of profile.

    - **profile_file**: path to a profiles file. (string, default: given by AWS_CREDENTIAL_PROFILES_FILE environment varialbe, or ~/.aws/credentials).

    - **profile_name**: name of a profile. (string, default: `"default"`)

    ```
    [default]
    aws_access_key_id=YOUR_ACCESS_KEY_ID
    aws_secret_access_key=YOUR_SECRET_ACCESS_KEY

    [profile2]
    ...
    ```

  - "properties": uses aws.accessKeyId and aws.secretKey Java system properties.

  - "anonymous": uses anonymous access. This auth method can access only public files.

  - "session": uses temporary-generated access_key_id, secret_access_key and session_token.

    - **access_key_id**: AWS access key ID (string, required)

    - **secret_access_key**: AWS secret access key (string, required)

    - **session_token**: session token (string, required)

* **path_match_pattern**: regexp to match file paths. If a file path doesn't match with this pattern, the file will be skipped (regexp string, optional)

* **total_file_count_limit**: maximum number of files to read (integer, optional)

## Example

```yaml
in:
  type: s3
  bucket: my-s3-bucket
  path_prefix: logs/csv-
  endpoint: s3-us-west-1.amazonaws.com
  access_key_id: ABCXYZ123ABCXYZ123
  secret_access_key: AbCxYz123aBcXyZ123
```

To skip files using regexp:

```yaml
in:
  type: s3
  bucket: my-s3-bucket
  path_prefix: logs/csv-
  # ...
  path_match_pattern: \.csv$   # a file will be skipped if its path doesn't match with this pattern

  ## some examples of regexp:
  #path_match_pattern: /archive/         # match files in .../archive/... directory
  #path_match_pattern: /data1/|/data2/   # match files in .../data1/... or .../data2/... directory
  #path_match_pattern: .csv$|.csv.gz$    # match files whose suffix is .csv or .csv.gz
```

To use AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables:

```yaml
in:
  type: s3
  bucket: my-s3-bucket
  path_prefix: logs/csv-
  endpoint: s3-us-west-1.amazonaws.com
  auth_method: env
```

For public S3 buckets such as `landsat-pds`:

```yaml
in:
  type: s3
  bucket: landsat-pds
  path_prefix: scene_list.gz
  auth_method: anonymous
```

## Build

```
./gradlew gem
```

## Test

To run unit tests, we need to configure the following environment variables.
```
EMBULK_S3_TEST_BUCKET
EMBULK_S3_TEST_ACCESS_KEY_ID
EMBULK_S3_TEST_SECRET_ACCESS_KEY
```
