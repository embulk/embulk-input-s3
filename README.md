# S3 file input plugin for Embulk

## Overview

* Plugin type: **file input**
* Resume supported: **yes**
* Cleanup supported: **yes**

## Configuration

- **bucket** S3 bucket name (string, required)
- **path_prefix** prefix of target keys (string, required)
- **endpoint** S3 endpoint login user name (string, optional)
- **access_key_id** AWS access key id (string, required if no `credential_provider` given)
- **secret_access_key** AWS secret key (string, required if no `credential_provider` given)
- **credential_provider** (string, optional)

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

### Credentials Provider

Also Credentials provider can be specified with `credential_provider`.

See [Providing AWS Credentials in the AWS SDK for Java](http://docs.aws.amazon.com/en_us/AWSSdkDocsJava/latest//DeveloperGuide/credentials.html) for details of each provider.

provider | type
-------- | -----
Default(looks for credentials in the follwing  order:  env -> system -> profile -> instance)  | default
Java System Property | system
Environment Variable | env
Profile | profile
Instance Profile | instance


## Build

```
./gradlew gem
```

