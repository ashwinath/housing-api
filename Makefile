commit=$(shell git rev-parse HEAD)
all: build push

build:
	docker build -t $(REGISTRY)/housing-api:$(commit) -t $(REGISTRY)/housing-api:latest .

push:
	docker push $(REGISTRY)/housing-api:$(commit)
	docker push $(REGISTRY)/housing-api:latest
