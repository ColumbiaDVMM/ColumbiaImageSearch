import happybase

# gives error
# TSocket read 0 bytes
# [Errno 32] Broken pipe

if __name__ == "__main__":
	pool = happybase.ConnectionPool(size=8,host="10.1.94.57")
	with pool.connection() as conn:
		table_name = "escorts_images_sha1_infos_dev"
		hbase_table = conn.table(table_name)
		batch_list_queries = ["000421227D83DA48DB4A417FCEFCA68272398B8E"]
		rows = hbase_table.rows(batch_list_queries)
		print rows
	
