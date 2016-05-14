#!/bin/bash
# Run tests suite, you can specify which nose to run by setting environment
# variable `nose` (running without will use `nosetests`).
#   nose=nosetests-3.2 ./run-tests.sh

# Exit on error
set -e

echo "[$(date +%Y%m%dT%H%M%S)] ${USER}@$(hostname) :: $(python --version 2>&1)"
echo


echo "running flake8"
flake8 fastavro tests
if [ $? -eq 0 ]
then echo "OK"
else echo "FAILED"
fi
echo

nose=${nose-nosetests}
echo "running $nose"

${nose} -vd $@ tests
