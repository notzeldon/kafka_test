# Kafka Test App

This is a test app. [maybe will paste more info for good repo description later]

### Run app

Clone repo and run command

```bash
docker compose up --build -d
```

### View consumer

For viewing message, you can use command

```bash
docker logs python_app -f
```

or run cunsumer manually, using command in docker container

```bash
docker exec -it python_app python3 main.py consume --topic hello_topic --kafka kafka:9093
```

> You need create topic, if it's not exists. Use next command 
> ```bash
> docker exec -it python_app python3 main.py init --topic hello_topic --kafka kafka:9093
> ```

### Produce a message

```bash
docker exec -it python_app python3 main.py produce --message "Hello Message" --topic hello_topic --kafka kafka:9093 
```