FROM golang:latest as builder

ENV GO111MODULE=on
ARG command

WORKDIR /build
COPY go.mod .
COPY go.sum .

RUN go mod download

ADD . .

WORKDIR cmd/${command}

RUN CGO_ENABLED=0 go build 

FROM golang:1.11.2-alpine3.8
ARG command
COPY --from=builder /build/cmd/${command}/${command} .

