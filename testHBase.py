# http://happybase.readthedocs.org/en/latest/user.html
import happybase,sys
import numpy as np
connection = happybase.Connection('10.1.94.57')
# This fails...
#alltables = connection.tables()
tab = connection.table('aaron_memex_ht-images')
if __name__ == '__main__':

  if len(sys.argv)>1:
	my_ids_int= [int(sys.argv[1])]
  else:
	my_ids_int = [8496680,100000000]
	my_ids_int = list(np.random.randint(0,107415056,5))
  my_ids_str = [str(one_id) for one_id in my_ids_int]
  print my_ids_str
  all_rows=tab.rows(my_ids_str)
  for one_row in all_rows:
	#print one_row
	try:
		print one_row[0],"has "+str(len(one_row[1]['meta:columbia_near_dups'].split(",")))+" near duplicate ids:", one_row[1]['meta:columbia_near_dups']
		print one_row[0],"Near duplicate distances:",one_row[1]['meta:columbia_near_dups_dist']
		print one_row[0],"Image URL:",one_row[1]['meta:location']
	except:
		try: 
			img64=one_row[1]['image:orig']
			print one_row[0],"not yet indexed. Call the Colubmia Search API if you really need the similar images."
		except:
			print one_row[0],"may have failed to be downloaded and could be no longer available, hence we have no hash codes or features."
