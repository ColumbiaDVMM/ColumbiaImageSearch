from cdr_ingester import CDRIngester

if __name__=="__main__":
	conf_file="../../conf/global_var.json"
	CDRIn = CDRIngester(conf_file)
	CDRIn.set_start(0)
	CDRIn.set_batch_size(10)
	CDRIn.set_verbose(2)
	batch = CDRIn.get_batch()
	print batch
