#!/bin/sh

ipcs -m | grep 0x020202 | awk '{print "ipcrm -M ", $1}' | sh
ipcs -m | grep 0x030303 | awk '{print "ipcrm -M ", $1}' | sh

