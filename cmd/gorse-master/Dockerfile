############################
# STEP 1 build executable binary
############################
FROM golang:1.22 AS builder

ENV GOPROXY=https://goproxy.cn,direct
WORKDIR /src/gorse
COPY . /src/gorse

RUN go mod download \
    && go install github.com/grpc-ecosystem/grpc-health-probe@v0.4.16

RUN cd cmd/gorse-master && \
    CGO_ENABLED=0 go build -ldflags=" \
       -X 'github.com/zhenghaoz/gorse/cmd/version.Version=$(git describe --tags $(git rev-parse HEAD))' \
       -X 'github.com/zhenghaoz/gorse/cmd/version.GitCommit=$(git rev-parse HEAD)' \
       -X 'github.com/zhenghaoz/gorse/cmd/version.BuildTime=$(date)'" . && \
    mv gorse-master /usr/bin

RUN /usr/bin/gorse-master --version

############################
# STEP 2 build a small image
############################
FROM scratch

# copy executable files
COPY --from=builder /go/bin/grpc-health-probe  /usr/local/bin/grpc-health-probe
COPY --from=builder /usr/bin/gorse-master /app/gorse-master

WORKDIR /app

ENV USER=root

ENTRYPOINT ["/usr/bin/gorse-master", "-c", "/etc/gorse/config.toml"]
