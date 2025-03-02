FROM docker.io/mysql:latest

COPY ./docker/certs/server.key /etc/mysql/certs/server.key
COPY ./docker/certs/server.crt /etc/mysql/certs/server.crt
COPY ./docker/certs/kdbc-test-sslCA.crt /etc/mysql/certs/kdbc-test-sslCA.crt

RUN chmod 600 /etc/mysql/certs/server.key && chown mysql:mysql /etc/mysql/certs/server.key
RUN chmod 644 /etc/mysql/certs/server.crt && chown mysql:mysql /etc/mysql/certs/server.crt
RUN chmod 600 /etc/mysql/certs/kdbc-test-sslCA.crt && chown mysql:mysql /etc/mysql/certs/kdbc-test-sslCA.crt

ENTRYPOINT ["docker-entrypoint.sh"]

CMD [ "--ssl-cert=/etc/mysql/certs/server.crt", \
    "--ssl-key=/etc/mysql/certs/server.key", \
    "--ssl-ca=/etc/mysql/certs/kdbc-test-sslCA.crt", \
    "--local-infile=TRUE"]
