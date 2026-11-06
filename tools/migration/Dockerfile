#build
FROM golang:1.27.3-alpine3.24@sha256:fa3d596898d9f126af7c01d14ee7597bab3f39cccf392ef36b69fe77cd681f18 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

# copy go sources
COPY pkg/ pkg/
COPY main.go main.go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -o migration-cleaner main.go

# Use alpine tiny images as a base
FROM alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

ENV USER_UID=2001 \
    USER_NAME=cleaner \
    GROUP_NAME=cleaner

WORKDIR /

COPY --from=builder --chown=${USER_UID} /app/migration-cleaner .

RUN addgroup ${GROUP_NAME} && adduser -D -G ${GROUP_NAME} -u ${USER_UID} ${USER_NAME}
USER ${USER_UID}

ENTRYPOINT ["/migration-cleaner"]
