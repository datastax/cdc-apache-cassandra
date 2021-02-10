#!/usr/bin/env bash
# kb get pod -l app=elassandra -o jsonpath="{.items[*].spec.containers[*].image}"
# kubectl get pod -l app=elassandra '-o=custom-columns=NAME:.metadata.name,IMAGE:.spec.containers[*].image'


NS=${NS:-"ns7"}

source integ-test/test-lib.sh
setup_flavor

test_start
install_elassandra_datacenter $NS cl1 dc1 3
java/edctl/build/libs/edctl watch-dc -n elassandra-cl1-dc1 -ns $NS --health GREEN -r 3

kb delete pod elassandra-cl1-dc1
kb delete pvc data-volume-elassandra-cl1-dc1-0-0

java/edctl/build/libs/edctl watch-dc -n elassandra-cl1-dc1 -ns $NS --health GREEN -r 3
kb exec elassandra-cl1-dc1 -- nodetool status

# cleanup
uninstall_elassandra_datacenter $NS cl1 dc1
echo "Test SUCCESSFUL"
