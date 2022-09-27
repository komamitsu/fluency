#!/usr/bin/env bash

set -eu

unix_domain_socket_path=/tmp/fluency-test-unix-domain-socket

# Start Fluentd as a daemon
rm -f fluentd.log
ruby -rerb -e "unix_domain_socket_path = '$unix_domain_socket_path'; puts ERB.new(File.read('fluentd.conf.erb')).result(binding)" > fluentd.conf
fluentd -d fluentd.pid -c fluentd.conf -l fluentd.log
trap 'pkill -F fluentd.pid' EXIT

pushd app
./gradlew installDist

build/install/fluency-test-unix-domain-socket/bin/fluency-test-unix-domain-socket my-server $unix_domain_socket_path fluency.test
popd

sleep 5

if grep 'forwarded.fluency.test:' fluentd.log; then
    exit 0
fi

echo "Test failed."
egrep '\[(error|warn)\]' fluentd.log

exit 1

