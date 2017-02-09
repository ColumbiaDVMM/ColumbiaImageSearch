import os,sys
# http://happybase.readthedocs.org/en/latest/user.html
import happybase

connection = happybase.Connection('10.1.94.57')

suffix='_2015_oct_nov'
# need to create these tables
tab_ht_images_infos='ht_images_infos'+suffix # need to create it
# end tables to be created

if __name__ == '__main__':

    tab = connection.table(tab_ht_images_infos)
    for one_row in tab.scan():
        if not one_row[1]['info:all_parent_ids']:
            print "Missing 'info:all_parent_ids' in row {}.".format(one_row[0])
    print "Done."


    
