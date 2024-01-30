FROM arm32v7/golang:1.19-alpine as builder
WORKDIR /home/sql-connector

COPY . .
RUN go build -o main main.go  

FROM arm32v7/alpine
WORKDIR /home/sql-connector
COPY --from=builder /home/sql-connector/main .

CMD [ "/home/sql-connector/main" ]
