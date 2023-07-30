
FROM golang:1.19


WORKDIR /app


COPY go.mod .
COPY go.sum .


RUN go mod download


COPY . .
ENV DOCKER_ENV=docker_env 

RUN go build -o main .


CMD ["./main"]