#!/bin/sh

ipcs -m | grep 0x20202 | awk '{print "ipcrm -M ", $3}' | sh
ipcs -m | grep 0x30303 | awk '{print "ipcrm -M ", $3}' | sh
ipcs -m | grep 0x2016 | awk '{print "ipcrm -M ", $3}' | sh
ipcs -m | grep 0x3016 | awk '{print "ipcrm -M ", $3}' | sh



rm -rf ./example_kvsvr_1
rm -rf ./example_kvsvr_2
