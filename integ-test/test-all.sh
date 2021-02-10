#!/usr/bin/env bash

set -oe pipefail

integ-test/test-admission.sh
integ-test/test-hostnetwork.sh
integ-test/test-reaper-registration.sh
integ-test/test-scaleup-park-unpark.sh
integ-test/test-multiple-dc-1ns.sh
integ-test/test-multiple-dc-2ns.sh
integ-test/test-rolling-upgrade.sh