ARG BASE_IMAGE=ubuntu:25.10
FROM ${BASE_IMAGE}

ADD oteldb     /usr/local/bin/oteldb
ADD odbbackup  /usr/local/bin/odbbackup
ADD odbrestore /usr/local/bin/odbrestore
ADD odbmigrate /usr/local/bin/odbmigrate
ADD odbingest  /usr/local/bin/odbingest
ADD odbselect  /usr/local/bin/odbselect

ENTRYPOINT ["oteldb"]
