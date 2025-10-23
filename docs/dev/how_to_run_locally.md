# How to run locally

## Tags in source code

There are some tags in the code to track different things:

- FIXME - need to fix it asap
- TODO - task which we shouldn't forget
- OPTIMIZE - have to think about best way
- TEST - only for testing code, it mustn't be in production

If you write code in VSCode and use Todo Tree extension, you can add custom tags

`Ctrl + Shift + P`, `Todo Tree: Add tag` and just write tag name in upper case

## How to test locally

- Run two docker containers: **Postgres** and **Minio**

  ```bash
  docker run --rm \
  -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres -d postgres
  ```

  ```bash
  docker run -d \
   -p 9000:9000 \
   -p 9001:9001 \
   --name minio \
   -v ~/minio/data:/data \
   -e "MINIO_ROOT_USER=rootuser" \
   -e "MINIO_ROOT_PASSWORD=rootpassword" \
   quay.io/minio/minio server /data --console-address ":9001"
  ```

- Build `compactor` as Golang application

  ```bash
  go build -o ./bin/compactor -ldflags="-w -s"
  ```

- Run `compactor` for specific date

  ```bash
  ./bin/compactor \
    --startdate 2024/03/12 --starttime 2024/03/12/13 \
    --minio.url="localhost:9000" --minio.key="rootuser" --minio.secret="rootpassword" \
    --pg.url="postgres://postgres:postgres@localhost:5432/postgres" > logs/log.txt
  ```

The compactor does not require downloading files from the generator to the minio, so you can omit the flags for the minio

- After execution, check Minio Storage.

  Open <http://localhost:9001> in your browser, use credentials from `docker run` command

## Integration tests

### How to run integration tests

Use the default go command for tests in compactor root directory

```bash
go test ./...
```
