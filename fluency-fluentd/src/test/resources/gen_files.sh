#!/usr/bin/env bash

set -eux

# Create server private key
openssl genrsa \
    -out test_server.key

# Create server csr
openssl req \
    -new \
    -key test_server.key \
    -subj '/CN=localhost' \
    -out test_server.csr

# Create server certification
openssl x509 \
    -req \
    -sha256 \
    -in test_server.csr \
    -signkey test_server.key \
    -out test_server.crt

# Create Keystore for TLS server in tests
openssl pkcs12 \
    -inkey test_server.key \
    -in test_server.crt \
    -export \
    -password pass:p12pass \
    -out keystore.pkcs12 \

rm -f keystore.jks
keytool \
    -importkeystore \
    -srcstorepass p12pass \
    -srckeystore keystore.pkcs12 \
    -srcstoretype pkcs12 \
    -destkeystore keystore.jks \
    -destkeypass keypassword \
    -deststorepass keypassword

# Create Truststore for TLS client in tests
rm -f truststore.jks
keytool \
    -import \
    -noprompt \
    -file test_server.crt \
    -alias mytruststore \
    -keystore truststore.jks \
    -storepass trustpassword

