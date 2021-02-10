#!/usr/bin/env bash

source integ-test/test-lib.sh
setup_flavor

NS="ns1"

install_cassandra_datacenter $NS cl1 dc1 1

# create pulsar topics, subscription

# create an elasticsearch cluster

# cleanup
uninstall_cassandra_datacenter $NS cl1 dc1
echo "Test SUCCESSFUL"
