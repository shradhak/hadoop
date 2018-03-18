#!/usr/bin/python
#reducer.py
import sys

quakes={}

#Partitoner
for line in sys.stdin:
    line = line.strip()
	qdate,mag=line.split('\t')
	if qdate in quakes:
	quakes[qdate].append(int(mag))
    else:
		quakes[qdate]=[]
		quakes[qdate].append(int(mag))

#Reducer

for dates in quakes.keys():

    totalmag=sum(quakes[dates])

    print '%s,%s' %(dates,totalmag)
