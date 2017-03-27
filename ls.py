from __future__ import print_function
import time
import sys

def long_sleep(sleeptime):
	starttime = time.time() 
	while time.time() - starttime < sleeptime:
		print("waiting, %i seconds         \r"%int(starttime + sleeptime - time.time()), end='')
		sys.stdout.flush()
		time.sleep(0.1)
	print('\n')
		
def range_with_status(total):
    """ iterate from 0 to total and show progress in console """
    n=0
    while n<total:
        done = '#'*(n+1)
        todo = '-'*(total-n-1)
        s = '<{0}>'.format(done+todo)
        if not todo:
            s+='\n'        
        if n>0:
            s = '\r'+s
        print(s, end='')
        sys.stdout.flush()
        yield n
        n+=1

#long_sleep(10)

# example for use of status generator
for i in range_with_status(10):
    time.sleep(0.1)
