FROM golang:1.24-alpine

LABEL org.opencontainers.image.title="Kanban Tool" \
      org.opencontainers.image.description="Personal kanban board with Freshservice helpdesk integration" \
      org.opencontainers.image.source="https://github.com/TeeJS/golang-kanban" \
      org.opencontainers.image.url="https://github.com/TeeJS/golang-kanban"

WORKDIR /app

COPY . .

RUN go mod tidy && go build -o kanban

EXPOSE 17808

CMD ["./kanban"]
