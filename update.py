import os, time
lasttime = 0
interval = 3600
while 1:
	ctime = time.time()
	time_lapse = ctime-lasttime
	if time_lapse < interval:
		print 'sleep for',interval-time_lapse, 'seconds...'
		time.sleep(interval-time_lapse)
	lasttime=time.time()
	os.system('python incUp.py -l 200000 &>> logUpdate.txt')

