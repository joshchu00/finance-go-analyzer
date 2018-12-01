FROM joshchu00/golang-kafka:1.11-alpine
Add . .
RUN go build
CMD main