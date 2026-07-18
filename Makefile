DC = docker compose
EXEC = docker exec -it
LOGS = docker logs
ENV = --env-file .env
APP_FILE = docker_compose/app.yaml
STORAGES_FILE = docker_compose/storages.yaml
KAFKA_FILE = docker_compose/kafka.yaml
PROMETHEUS_FILE = docker_compose/prometheus.yaml
APP_CONTAINER = main-app

.PHONY: app
app:
	$(DC) -f $(APP_FILE) $(ENV) up --build -d

.PHONY: kafka
kafka:
	$(DC) -f $(KAFKA_FILE) $(ENV) up --build -d

.PHONY: storages
storages:
	$(DC) -f $(STORAGES_FILE) $(ENV) up --build -d

.PHONY: all
all:
	$(DC) -f $(STORAGES_FILE) -f $(APP_FILE) -f $(KAFKA_FILE) -f $(PROMETHEUS_FILE) $(ENV) up --build -d

.PHONY: app-down
app-down:
	$(DC) -f $(APP_FILE) down

.PHONY: storages-down
storages-down:
	$(DC) -f $(STORAGES_FILE) down

.PHONY: app-shell
app-shell:
	$(EXEC) $(APP_CONTAINER) bash

.PHONY: app-logs
app-logs:
	$(LOGS) $(APP_CONTAINER) -f

.PHONY: all-down
all-down:
	$(DC) -f $(STORAGES_FILE) -f $(APP_FILE) -f $(KAFKA_FILE) -f $(PROMETHEUS_FILE) down

.PHONY: kafka-down
kafka-down:
	$(DC) -f $(KAFKA_FILE) down

.PHONY: kafka-logs
kafka-logs:
	$(DC) -f $(KAFKA_FILE) logs -f

.PHONY: prometheus
prometheus:
	$(DC) -f $(PROMETHEUS_FILE) -f $(APP_FILE) -f $(KAFKA_FILE) $(ENV) up --build -d

.PHONY: prometheus-down
prometheus-down:
	$(DC) -f $(PROMETHEUS_FILE) -f $(APP_FILE) -f $(KAFKA_FILE) down

.PHONY: prometheus-logs
prometheus-logs:
	$(DC) -f $(PROMETHEUS_FILE) -f $(APP_FILE) logs -f
