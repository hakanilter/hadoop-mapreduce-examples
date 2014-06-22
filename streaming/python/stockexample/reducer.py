#!/usr/bin/env python

import sys

current_symbol = None
current_max = 0

for line in sys.stdin:
    line = line.strip()

    symbol, price = line.split('\t', 1)

    try:
        max = float(price)
    except ValueError:
        continue

    if current_symbol == symbol:
        if max > current_max:
            current_max = max
    else:
        if current_symbol != None:
            print '%s\t%s' % (current_symbol, current_max)
        current_symbol = symbol
        current_max = max

if current_symbol == symbol:
    print '%s\t%s' % (current_symbol, current_max)