ARG OPENJDK_TAG=8u212
FROM openjdk:${OPENJDK_TAG}

ARG SBT_VERSION=1.3.3

# Install sbt
RUN \
  curl -L -o sbt-$SBT_VERSION.deb https://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb

RUN apt-get update && apt-get install -y \
      mysql-client \
      sbt \
  && rm -rf /var/lib/apt/lists/*

RUN sbt sbtVersion
