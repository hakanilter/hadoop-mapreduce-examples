#!/usr/bin/env python

import sys

for line in sys.stdin:
    line = line.strip()
    columns = line.split('\t')
    if columns[0] != 'NYSE':
        exit

    symbol = columns[1]
    price = columns[4]
    print '%s\t%s' % (symbol, price)
