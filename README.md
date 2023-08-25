# store

## Tests

```bash
# test os store
go test -v -run=TestOSStore

# test s3 store
export S3_STORE_CONFIG=s3-config.json
go test -v -run=TestS3Store

# test qiniu store
export QINIU_STORE_CONFIG=qiniu-config.toml
go test -v -run=TestQiniuStore
```