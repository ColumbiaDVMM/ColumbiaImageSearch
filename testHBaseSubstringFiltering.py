# http://happybase.readthedocs.org/en/latest/user.html
import happybase,sys
connection = happybase.Connection('10.1.94.57')
tab = connection.table('ht_columbia_similar_images_sample')
Sha1Filter="RowFilter ( =, 'substring:{}')"

if __name__ == '__main__':

  if len(sys.argv)>1:
	sha1= str(sys.argv[1])
  else:
	sha1 = "0006EB748A2708AFA8B6AACE8E2AD3CA1B64B0A9"
  print Sha1Filter.format(sha1)
  for one_row in tab.scan(filter=Sha1Filter.format(sha1)):
	print one_row
  # Nope.
  #rows = tab.rows(filter=Sha1Filter.format(sha1))
  #print rows
