#!/usr/bin/env python3
import paytak

@paytak.wrap
def add(first, second):
    return first + second

@paytak.wrap
def inc(x):
    return x + 1

x = inc(2)
y = inc(4)

result = add(x, y)

paytak.debug_dump(result)

print(paytak.execute_dummy(result))
