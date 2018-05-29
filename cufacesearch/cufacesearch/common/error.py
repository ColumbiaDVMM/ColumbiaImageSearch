def full_trace_error(err_msg):
  import sys, traceback
  print(err_msg)
  exc_type, exc_value, exc_traceback = sys.exc_info()
  print("*** print_tb:")
  traceback.print_tb(exc_traceback, file=sys.stdout)
  print("*** print_exception:")
  traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
  sys.stdout.flush()