SHELL := /bin/bash

##############################
# BUILD
#
VERSION := $(shell sbt 'export version' | tail -n 1)
export VERSION

default: build

.PHONY: git-commit-template
git-commit-template: .commit_template
	@git config commit.template .commit_template

.PHONY: version
version:
	@echo $(VERSION)

.PHONY: clean
clean:
	sbt clean

.PHONY: style
style:
	sbt scalastyle

.PHONY: build
build: clean
	sbt assembly

.PHONY: test
test:
	sbt test

.PHONY: docs
docs: clean
	sbt unidoc

.PHONY: publish
publish:
	sbt publishSigned \
	aws s3api put-object --bucket download.memsql.com --key memsql-spark-interface-$(VERSION)/memsql-spark-interface-$(VERSION).jar --body interface/target/scala-2.10/MemSQL\ Spark\ Interface-assembly-$(VERSION).jar --acl public-read

.PHONY: publish-docs
publish-docs:
	sbt unidoc makeSite ghpagesPushSite

.PHONY: release
release: publish publish-docs
	sbt sonatypeRelease
