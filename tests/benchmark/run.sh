#!/usr/bin/env bash

set -eu -o pipefail

record_count=10000000
if [[ $# -gt 0 ]]; then
  record_count=$1
fi
echo "RECORD_COUNT: $record_count"

# Start Fluent-bit 
contaier_id=$(docker run -d -p 24224:24224 -v ./fluent-bit.conf:/fluent-bit.conf fluent/fluent-bit -v -c /fluent-bit.conf)
trap "docker stop $contaier_id" EXIT

pushd app
./gradlew installDist

duration_in_millis=$(build/install/fluency-benchmark/bin/fluency-benchmark fluency.test $record_count | grep '^DURATION:' | cut -d: -f2)
popd

echo "RPS: $(echo "$record_count / ($duration_in_millis / 1000.0)" | bc -l)"
