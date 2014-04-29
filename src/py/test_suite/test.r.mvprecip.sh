#!/bin/sh

DB="mvdb"
#../r.mvprecip.py -r database=$DB baseltime=baseltime.ref

../r.mvprecip.py -g database=$DB \
    fromtime="2013-09-09 06:00:00" totime="2013-09-09 06:03:00" \
    baseltime=baseltime.ref color=radarcolor.ref lignore=ignore.ref

exit 0
