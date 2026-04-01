# build
FROM golang:1.26.1-alpine3.22@sha256:07e91d24f6330432729082bb580983181809e0a48f0f38ecde26868d4568c6ac AS builder

WORKDIR /generator

COPY go.mod go.sum ./
RUN go mod download

# copy go sources
COPY cmd/ cmd/
COPY pkg/ pkg/
COPY main.go main.go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -o generator main.go

# Use alpine tiny images as a base
FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659

ENV USER_UID=2001 \
    USER_NAME=generator \
    GROUP_NAME=generator

WORKDIR /

COPY --from=builder --chown=${USER_UID} /generator/generator .

RUN addgroup ${GROUP_NAME} && adduser -D -G ${GROUP_NAME} -u ${USER_UID} ${USER_NAME}
USER ${USER_UID}

ENTRYPOINT ["/generator"]
