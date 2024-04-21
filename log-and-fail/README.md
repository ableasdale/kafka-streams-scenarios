# Log and Fail Scenario

Exploring what will happen when a Kafka Streams application is configured with the `LogAndFailExceptionHandler`.

## Start the broker

```bash
docker-compose up -d
```

## Troubleshooting

```bash
docker-compose exec broker kafka-topics --list --bootstrap-server broker:9092
```

Cleanup

```bash
docker-compose down && docker container prune && docker-compose up -d
```