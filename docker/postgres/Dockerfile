FROM docker.io/postgis/postgis:16-3.4-alpine

COPY ./docker/postgres/certs/server.key /var/lib/postgresql
COPY ./docker/postgres/certs/server.crt /var/lib/postgresql

COPY ./docker/postgres/certs/kdbc-test-sslCA.crt /var/lib/postgresql
COPY ./docker/postgres/certs/kdbc-test-sslCA.crl /var/lib/postgresql

RUN chown 0:70 /var/lib/postgresql/server.key && chmod 640 /var/lib/postgresql/server.key
RUN chown 0:70 /var/lib/postgresql/server.crt && chmod 640 /var/lib/postgresql/server.crt

RUN chown 0:70 /var/lib/postgresql/kdbc-test-sslCA.crt && chmod 640 /var/lib/postgresql/kdbc-test-sslCA.crt
RUN chown 0:70 /var/lib/postgresql/kdbc-test-sslCA.crl && chmod 640 /var/lib/postgresql/kdbc-test-sslCA.crl

ENTRYPOINT ["docker-entrypoint.sh"]

CMD [ "-c", "ssl=on" , "-c", "ssl_cert_file=/var/lib/postgresql/server.crt", "-c",\
    "ssl_key_file=/var/lib/postgresql/server.key", "-c",\
    "ssl_ca_file=/var/lib/postgresql/kdbc-test-sslCA.crt", "-c", "ssl_crl_file=/var/lib/postgresql/kdbc-test-sslCA.crl" ]
