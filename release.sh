#!/bin/bash

set -x
set -e
set -o pipefail

# Requirements: mvn, git, github cli
mvn clean package

pushd target/
tar -zcf bft-smart.tar.gz BFT-SMaRt-parallel-cp-1.0-SNAPSHOT.jar
popd

pushd ..
rm -f emulab.tar.gz
tar -zcf emulab.tar.gz emulab/
popd

gh release upload 1.0-beta target/bft-smart.tar.gz ../emulab.tar.gz --clobber --repo hensg/emulab-parallel-checkpoint
