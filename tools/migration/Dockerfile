#build
FROM golang:1.26.1-alpine3.22@sha256:07e91d24f6330432729082bb580983181809e0a48f0f38ecde26868d4568c6ac AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

# copy go sources
COPY pkg/ pkg/
COPY main.go main.go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -o migration-cleaner main.go

# Use alpine tiny images as a base
FROM alpine:3.22.1@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1

ENV USER_UID=2001 \
    USER_NAME=cleaner \
    GROUP_NAME=cleaner

WORKDIR /

COPY --from=builder --chown=${USER_UID} /app/migration-cleaner .

RUN addgroup ${GROUP_NAME} && adduser -D -G ${GROUP_NAME} -u ${USER_UID} ${USER_NAME}
USER ${USER_UID}

ENTRYPOINT ["/migration-cleaner"]
