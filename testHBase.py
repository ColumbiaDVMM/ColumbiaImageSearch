# http://happybase.readthedocs.org/en/latest/user.html
import happybase,sys
connection = happybase.Connection('10.1.94.57')
tab = connection.table('aaron_memex_ht-images')
if __name__ == '__main__':

  if len(sys.argv)>1:
	my_ids_int= [int(sys.argv[1])]
  else:
	my_ids_int = [8496680,100000000]
  my_ids_str = [str(one_id) for one_id in my_ids_int]
  all_rows=tab.rows(my_ids_str)
  for one_row in all_rows:
	#print one_row
	try:
		print one_row[0],"Near duplicate ids:", one_row[1]['meta:columbia_near_dups']
		print one_row[0],"Near duplicate distances:",one_row[1]['meta:columbia_near_dups_dist']
		print one_row[0],"Image URL:",one_row[1]['meta:location']
	except:
		print one_row[0],"not yet indexed. Call the Colubmia Search API if you really need the similar images."
