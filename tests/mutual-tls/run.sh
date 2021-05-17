#!/usr/bin/env bash

set -eu

if ! egrep '^127.0.0.1\s+my-server$' /etc/hosts; then
    echo "Please add '127.0.0.1 my-server' to /etc/hosts"
    exit 1
fi

rm -rf ./files

mkdir -p files

pushd files

# Create CA certificate
openssl req \
    -new \
    -x509 \
    -nodes \
    -subj '/CN=my-ca' \
    -keyout ca.key \
    -out ca.crt

# Create server private key
openssl genrsa \
    -out server.key

# Create server csr
openssl req \
    -new \
    -key server.key \
    -subj '/CN=my-server' \
    -out server.csr

# Create server certification
openssl x509 \
    -req \
    -in server.csr \
    -CA ca.crt \
    -CAkey ca.key \
    -CAcreateserial \
    -out server.crt

# Create client private key
openssl genrsa \
    -out client.key

# Create client csr
openssl req \
    -new \
    -key client.key \
    -subj '/CN=my-client' \
    -out client.csr

# Create client certification
openssl x509 \
    -req \
    -in client.csr \
    -CA ca.crt \
    -CAkey ca.key \
    -CAcreateserial \
    -out client.crt

# Create Keystore for client Java application
openssl pkcs12 \
    -inkey client.key \
    -in client.crt \
    -export \
    -password pass:p12pass \
    -out keystore.pkcs12 \

keytool \
    -importkeystore \
    -srcstorepass p12pass \
    -srckeystore keystore.pkcs12 \
    -srcstoretype pkcs12 \
    -destkeystore keystore.jks \
    -destkeypass keypassword \
    -deststorepass keypassword

# Create Truststore for client Java application
keytool \
    -import \
    -noprompt \
    -file server.crt \
    -alias mytruststore \
    -keystore truststore.jks \
    -storepass truststorepass

popd

# Start Fluentd as a daemon
rm -f fluentd.log
fluentd -d fluentd.pid -c fluentd.conf -l fluentd.log
trap 'kill $(cat fluentd.pid)' EXIT

pushd app
./gradlew installDist

export JAVA_OPTS='-Djavax.net.ssl.trustStore=../files/truststore.jks -Djavax.net.ssl.keyStorePassword=keypassword -Djavax.net.ssl.keyStore=../files/keystore.jks'
build/install/fluency-test-mutual-tls/bin/fluency-test-mutual-tls my-server 24224 fluency.test
popd

sleep 5

if grep 'forwarded.fluency.test:' fluentd.log; then
    exit 0
fi

echo "Test failed."
egrep '\[(error|warn)\]' fluentd.log

exit 1

