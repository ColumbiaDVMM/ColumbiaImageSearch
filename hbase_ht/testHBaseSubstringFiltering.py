# http://happybase.readthedocs.org/en/latest/user.html
import happybase,sys
connection = happybase.Connection('10.1.94.57')
#tab = connection.table('ht_columbia_similar_images_sample')
#tab = connection.table('escorts_images_similar_dev')
Sha1Filter="RowFilter ( =, 'substring:{}')"

if __name__ == '__main__':

  if len(sys.argv)>1:
	sha1= str(sys.argv[1])
  else:
	sha1 = "07AF10CCDBB5B3FE61EF65983B4FAF012FC98C0C"
  print Sha1Filter.format(sha1)
  for one_row in tab.scan(filter=Sha1Filter.format(sha1)):
	print one_row
  # Nope.
  #rows = tab.rows(filter=Sha1Filter.format(sha1))
  #print rows
