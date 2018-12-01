FROM joshchu00/golang-kafka:1.11-alpine
WORKDIR /var/lib
Add . .
RUN go build
CMD main