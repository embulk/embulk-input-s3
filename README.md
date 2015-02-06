# S3 file input plugin for Embulk

## Overview

* Plugin type: **file input**
* Rollback supported: **yes**
* Resume supported: **yes**
* Cleanup supported: **yes**

## Configuration

- **bucket** S3 bucket name (string, required)
- **path_prefix** prefix of target keys (string, required)
- **endpoint** S3 endpoint login user name (string, optional)
- **access_key_id** AWS access key id (string, required)
- **secret_access_key** AWS secret key (string, required)

## Example

```yaml
in:
  type: s3
  bucket: my-s3-bucket
  endpoint: s3-us-west-1.amazonaws.com
  access_key_id: ABCXYZ123ABCXYZ123
  secret_access_key: AbCxYz123aBcXyZ123
```

## Build

```
./gradlew gem
```

