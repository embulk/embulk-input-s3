# S3 file input plugin for Embulk

## Overview

* Plugin type: **file input**
* Resume supported: **yes**
* Cleanup supported: **yes**

## Configuration

- **bucket** S3 bucket name (string, required)

- **path_prefix** prefix of target keys (string, required)

- **direct_path_prefix_object** flag to enable treating `path_prefix` as a single object key (boolean, default to `false`). Since list-objects operation on S3 is eventually consistent, using this for a  more reliable way to retrieve a single object when you know the exact object key.

- **endpoint** S3 endpoint login user name (string, optional)

- **region** S3 region. endpoint will be in effect if you specify both of endpoint and region (string, optional)

- **http_proxy** http proxy configuration to use when accessing AWS S3 via http proxy. (optional)
  - **host** proxy host (string, required)
  - **port** proxy port (int, optional)
  - **https** use https or not (boolean, default true)
  - **user** proxy user (string, optional)
  - **password** proxy password (string, optional)

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

  - "default": uses AWS SDK's default strategy to look up available credentials from runtime environment. This method behaves like the combination of the following methods.

    1. "env"
    1. "properties"
    1. "profile"
    1. "instance"


* **path_match_pattern**: regexp to match file paths. If a file path doesn't match with this pattern, the file will be skipped (regexp string, optional)

* **total_file_count_limit**: maximum number of files to read (integer, optional)

* **min_task_size** (experimental): minimum bytesize of a task. If this is larger than 0, one task includes multiple input files up until it becomes the bytesize in total. This is useful if too many number of tasks impacts performance of output or executor plugins badly. (integer, optional)

* **incremental**: enables incremental loading. If incremental loading is enabled, config diff for the next execution will include `last_path` parameter so that next execution skips files before the path. Otherwise, `last_path` will not be included.

* **skip_glacier_objects**: if true, skip processing objects stored in Amazon Glacier (boolean, default false)

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

To set http proxy configuration:

```yaml
in:
  type: s3
  bucket: my-s3-bucket
  path_prefix: logs/csv-
  endpoint: s3-us-west-1.amazonaws.com
  access_key_id: ABCXYZ123ABCXYZ123
  secret_access_key: AbCxYz123aBcXyZ123
  http_proxy:
    host: localhost
    port: 8080
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

## Release

```
./gradlew clean gem classpath
./gradlew gemPush # release plugin gems to RubyGems.org
./gradlew bintrayUpload # release embulk-(input-s3|input-riak_cs|util-aws-credentials) to Bintray maven repo
```

## Test

To run unit tests, we need to configure the following environment variables.
```
EMBULK_S3_TEST_BUCKET
EMBULK_S3_TEST_ACCESS_KEY_ID
EMBULK_S3_TEST_SECRET_ACCESS_KEY
```
