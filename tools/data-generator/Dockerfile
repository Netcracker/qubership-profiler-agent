# build
FROM golang:1.25.7-alpine3.22@sha256:20c8a94b529a9a127b6990c5e03537bd71ce3ebfdd744741e96168ac338bd862 AS builder

WORKDIR /generator

COPY go.mod go.sum ./
RUN go mod download

# copy go sources
COPY cmd/ cmd/
COPY pkg/ pkg/
COPY main.go main.go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -o generator main.go

# Use alpine tiny images as a base
FROM alpine:3.22.1

ENV USER_UID=2001 \
    USER_NAME=generator \
    GROUP_NAME=generator

WORKDIR /

COPY --from=builder --chown=${USER_UID} /generator/generator .

RUN addgroup ${GROUP_NAME} && adduser -D -G ${GROUP_NAME} -u ${USER_UID} ${USER_NAME}
USER ${USER_UID}

ENTRYPOINT ["/generator"]
