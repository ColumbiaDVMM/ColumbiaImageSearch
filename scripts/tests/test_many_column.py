import happybase
import hashlib

if __name__=="__main__":
    conn = happybase.Connection(host='10.1.94.57')
    tab_name = 'test_many_columns'
    row_key = 'KEY_SHA1'
    try:
        conn.create_table(tab_name,{'c': dict()})
    except Exception as inst:
	print inst
    table = conn.table(tab_name)
    for cq in range(100000,1000000):
        hasher = hashlib.sha1()
        hasher.update(b'{}'.format(cq))    
    	table.put(row_key, {'c:{}'.format(hasher.hexdigest()): str(cq)})
    	row = table.row(row_key)
        print cq+1,len(row.keys())
