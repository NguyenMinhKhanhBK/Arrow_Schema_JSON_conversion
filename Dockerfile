FROM ubuntu:focal

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update -y -q && \
    apt install -y -q --no-install-recommends \
        build-essential \
        cmake \
        pkg-config \
        ca-certificates \ 
        lsb-release \
        wget \
        libgtest-dev \
        gnupg && \
    apt clean && rm -rf /var/lib/apt/lists*

RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

RUN apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

RUN apt update -y -q && \
    apt install -y -q libarrow-dev

WORKDIR /schema_json

COPY . .
