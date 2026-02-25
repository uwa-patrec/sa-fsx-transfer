# sa-fsx-transfer

S3/FSx file transfer container for the GPU processing pipeline. Downloads files from S3 to FSx on cheap CPU instances before GPU processing begins, and uploads results back to S3 afterward.

## Scripts

| Script      | Direction | Environment Variables                |
| ----------- | --------- | ------------------------------------ |
| download.py | S3 to FSx | `S3_BUCKET`, `S3_KEY`, `OUTPUT_PATH` |
| upload.py   | FSx to S3 | `INPUT_PATH`, `S3_BUCKET`, `S3_KEY`  |

## Environment Variables

### download.py

| Variable      | Required | Default      | Description                  |
| ------------- | -------- | ------------ | ---------------------------- |
| `S3_BUCKET`   | Yes      | -            | Source S3 bucket name        |
| `S3_KEY`      | Yes      | -            | Source S3 object key         |
| `OUTPUT_PATH` | No       | `/fsx/input` | Destination directory on FSx |

### upload.py

| Variable     | Required | Default | Description                  |
| ------------ | -------- | ------- | ---------------------------- |
| `INPUT_PATH` | Yes      | -       | Absolute path to file on FSx |
| `S3_BUCKET`  | Yes      | -       | Destination S3 bucket name   |
| `S3_KEY`     | Yes      | -       | Destination S3 object key    |

## Build Locally

```bash
docker build -t sa-fsx-transfer .
```

## Test Locally

```bash
# Download from S3
docker run --rm \
  -v /tmp/fsx:/fsx \
  -e S3_BUCKET=my-bucket \
  -e S3_KEY=videos/test.mp4 \
  -e OUTPUT_PATH=/fsx/input \
  -e AWS_ACCESS_KEY_ID=<key> \
  -e AWS_SECRET_ACCESS_KEY=<secret> \
  sa-fsx-transfer

# Upload to S3
docker run --rm \
  -v /tmp/fsx:/fsx \
  -e INPUT_PATH=/fsx/output/result.mp4 \
  -e S3_BUCKET=my-bucket \
  -e S3_KEY=results/result.mp4 \
  -e AWS_ACCESS_KEY_ID=<key> \
  -e AWS_SECRET_ACCESS_KEY=<secret> \
  sa-fsx-transfer python upload.py
```

## CI/CD

GitHub Actions builds and pushes the Docker image to ECR on every push to `main`.

Required repository secrets:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

Required repository variables:
- `AWS_ACCOUNT_ID`
- `AWS_REGION` (default: `ap-southeast-2`)
- `ECR_REPOSITORY_NAME`

## Performance

For a 5GB file:
- Single-threaded: ~60 seconds
- Optimized (10 threads): ~30 seconds
- Speed: ~80-160 MB/s depending on network
