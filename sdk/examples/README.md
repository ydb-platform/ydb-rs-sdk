# YDB SDK Examples

## Common dependencies for examples
All examples (but listed below) need local ydb started with docker-compose up with docker-compose.yaml from repository root dir.

## Additional dependencies for some examples
### auth-yc-cmdline
The auth-yc-cmdline.rs example need installed [yc cli](https://cloud.yandex.com/en/docs/cli/operations/install-cli) and active authentication to yandex cloud account.

### auth-ycloud-metadata
The auth-ycloud-metadata.rs example need to be run from Compute Engine in Yandex Cloud with service account - for receive auth token.

