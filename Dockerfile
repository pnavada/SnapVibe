FROM ubuntu:22.04

RUN apt-get update && apt install -y golang

WORKDIR /home/ubuntu

COPY snap.go .
COPY hostsfile.txt .

ENTRYPOINT [ "go", "run", "snap.go" ]