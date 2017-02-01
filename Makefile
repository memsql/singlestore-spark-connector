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
	sbt doc

.PHONY: publish
publish:
	sbt publishSigned

.PHONY: publish-docs
publish-docs:
	sbt doc makeSite ghpagesPushSite

.PHONY: release
release: publish publish-docs
	sbt sonatypeRelease
