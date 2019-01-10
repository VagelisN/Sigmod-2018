#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
valgrind -v --leak-check=full --show-leak-kinds=all ${DIR}/build/release/radixhash
