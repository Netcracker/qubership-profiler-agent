#build
FROM golang:1.26.2-alpine3.22@sha256:c259ff7ffa06f1fd161a6abfa026573cf00f64cfd959c6d2a9d43e3ff63e8729 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

# copy go sources
COPY pkg/ pkg/
COPY main.go main.go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -o migration-cleaner main.go

# Use alpine tiny images as a base
FROM alpine:3.23.4@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11

ENV USER_UID=2001 \
    USER_NAME=cleaner \
    GROUP_NAME=cleaner

WORKDIR /

COPY --from=builder --chown=${USER_UID} /app/migration-cleaner .

RUN addgroup ${GROUP_NAME} && adduser -D -G ${GROUP_NAME} -u ${USER_UID} ${USER_NAME}
USER ${USER_UID}

ENTRYPOINT ["/migration-cleaner"]
