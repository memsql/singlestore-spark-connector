SHELL := /bin/bash

##############################
# BUILD
#
VERSION := $(shell sbt 'export version' | tail -n 1)
export VERSION

.PHONY: version
version:
	@echo $(VERSION)

.PHONY: clean
clean:
	sbt clean
	sbt "project connectorLib" clean
	sbt "project etlLib" clean
	rm -rf distribution/

.PHONY: build
build: clean
	sbt "project connectorLib" assembly
	sbt "project etlLib" assembly
	sbt assembly

.PHONY: docs
docs: clean
	sbt "project connectorLib" doc
	sbt "project etlLib" doc

.PHONY: package
package: docs build
	mkdir -p distribution/dist/memsql-$(VERSION)
	cp README.md distribution/dist/memsql-$(VERSION)/
	cp target/scala-2.10/MemSQL-assembly-$(VERSION).jar distribution/dist/memsql-$(VERSION)/
	cp -r connectorLib/target/scala-2.10/api/ distribution/dist/memsql-$(VERSION)/docs/
	cp -r etlLib/target/scala-2.10/api/ distribution/dist/memsql-$(VERSION)/docs/
	cd distribution/dist; \
	tar cvzf memsql-$(VERSION).tar.gz memsql-$(VERSION)/
