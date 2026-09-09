#!/usr/bin/env bash
# Runs the whole demo pipeline end to end: build the image, start the stack, write
# data, validate it arrived. Run `05-teardown.sh` separately when done.
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${dir}/01-build-image.sh"
"${dir}/02-start-stack.sh"
"${dir}/03-insert-data.sh"
"${dir}/04-validate.sh"
