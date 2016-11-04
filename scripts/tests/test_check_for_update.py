import happybase

tab_name = "escorts_images_updates"
conn = happybase.Connection(host='10.1.94.57')
table_updateinfos = conn.table(tab_name)
index_batches = []

for row in table_updateinfos.scan(row_start='index_update_', row_stop='index_update_~'):
    if "info:indexed" not in row[1] and 'info:started' not in row[1] and 'info:corrupted' not in row[1]:
        print("Adding update: {}".format(row[0]))
        index_batches.append((row[0], row[1]["info:list_sha1s"]))

print("We have {} updates to process.".format(len(index_batches)))


