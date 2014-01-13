#!/usr/bin/python

s = nul

try:
  f = float(s)
  print('float: ' + str(f))
except ValueError:
  f = 0.0
  print('failed, float: ' + str(f))