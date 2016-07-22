import happybase

if __name__=="__main__":
	conn = happybase.Connection(host='10.1.94.57')
    tab_name = 'test_many_columns'
    row_key = 'KEY'
    conn.create_table(tab_name,{'c': dict()})
    table = conn.table(tab_image)
    for cq in range(1000):
    	table.put(row_key, {'c:{}'.format(cq): cq})
    	row = table.row(row_key)
        print cq+1,len(row.keys())
