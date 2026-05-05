FROM golang:1.24-alpine

WORKDIR /app

COPY . .

RUN go mod tidy && go build -o kanban

EXPOSE 17808

CMD ["./kanban"]
