#!/usr/bin/env bash

set -eu

unix_domain_socket_path=/tmp/fluency-test-unix-domain-socket
record_count=100000

rm -f $unix_domain_socket_path

# Start Fluentd as a daemon
rm -f fluentd.log
ruby -rerb -e "unix_domain_socket_path = '$unix_domain_socket_path'; puts ERB.new(File.read('fluentd.conf.erb')).result(binding)" > fluentd.conf
fluentd -d fluentd.pid -c fluentd.conf -l fluentd.log
trap 'pkill -F fluentd.pid' EXIT

pushd app
./gradlew installDist

build/install/fluency-test-unix-domain-socket/bin/fluency-test-unix-domain-socket my-server $unix_domain_socket_path fluency.test $record_count
popd

sleep 5

sum=$(grep 'fluency.test_count' fluentd.log | cut -d' ' -f5 | jq '."fluency.test_count"' | awk '{sum += $1} END {print sum}')

if [[ $sum = $record_count ]]; then
    exit 0
fi

echo "Test failed."
egrep '\[(error|warn)\]' fluentd.log

exit 1

