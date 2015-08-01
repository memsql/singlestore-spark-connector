SHELL := /bin/bash

SUPERAPP_VERSION := $(shell sbt 'export version' | tail -n 1)
export SUPERAPP_VERSION

.PHONY: version
version:
	@echo $(SUPERAPP_VERSION)

.PHONY: clean
clean:
	sbt clean

.PHONY:  build
build: clean
	sbt assembly
