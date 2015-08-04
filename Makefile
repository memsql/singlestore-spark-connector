SHELL := /bin/bash

##############################
# BUILD
#
MEMSQLRDD_VERSION := $(shell sbt 'export connectorLib/version' | tail -n 1)
export MEMSQLRDD_VERSION

.PHONY: version
version:
	@echo $(MEMSQLRDD_VERSION)

.PHONY: clean
clean:
	sbt clean
	sbt "project connectorLib" clean
	rm -rf distribution/

.PHONY: build
build: clean
	sbt "project connectorLib" assembly
	sbt assembly

.PHONY: docs
docs: clean
	sbt "project connectorLib" doc

.PHONY: m2publish
m2publish: clean
	sbt "project connectorLib" publish


.PHONY: package
package: docs build
	mkdir -p distribution/dist/memsqlrdd-$(MEMSQLRDD_VERSION)
	cp README.md distribution/dist/memsqlrdd-$(MEMSQLRDD_VERSION)/
	cp connectorLib/target/scala-2.10/memsql-spark-connector-assembly-$(MEMSQLRDD_VERSION).jar distribution/dist/memsqlrdd-$(MEMSQLRDD_VERSION)/
	cp -r connectorLib/target/scala-2.10/api/ distribution/dist/memsqlrdd-$(MEMSQLRDD_VERSION)/docs/
	cd distribution/dist; \
	tar cvzf memsqlrdd-$(MEMSQLRDD_VERSION).tar.gz memsqlrdd-$(MEMSQLRDD_VERSION)/
