#build
FROM golang:1.26.4-alpine3.22@sha256:727cfc3c40be55cd1bc9a4a059406b28a059857e3be752aa9d09531e12c20c56 AS builder

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
