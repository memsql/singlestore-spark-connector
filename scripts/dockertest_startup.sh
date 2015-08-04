#!/bin/bash
BINARY=$1
shift

# start sshd
sudo /usr/sbin/sshd

# run whatever the user wanted
exec $BINARY "$@"
