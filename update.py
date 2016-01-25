import os, time, datetime
lasttime = 0
interval = 3600

def checkProcessExist(cmdline):
  pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
  for pid in pids:
    try:
        pid_cmdline=open(os.path.join('/proc', pid, 'cmdline'), 'rb').read()
        if pid_cmdline.find(cmdline)>0:
                print pid,pid_cmdline
                return True
    except IOError: # proc has already terminated
        continue
  return False

while 1:
	ctime = time.time()
	time_lapse = ctime-lasttime
	if time_lapse < interval:
		print 'sleep for',interval-time_lapse, 'seconds...'
		time.sleep(interval-time_lapse)
	lasttime=time.time()
	cmdline="incUp.py"
  	print "Looking for "+cmdline
  	if checkProcessExist(cmdline):
        	print "Process "+cmdline+" exists!"
	else:
		print "Launching incremental update."
		os.system('python incUp.py -l 200000 >> logUpdate'+datetime.date.today().isoformat()+'.txt')

