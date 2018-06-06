import happybase

if __name__ == "__main__":
    hbase_conf = {"host": "10.108.16.137",
                  "table_name": "mx_ht_images_details_111k",
                  "clean_column_pattern": ["data:dlib*"]
                  }

    pool = happybase.ConnectionPool(size=1, host=hbase_conf["host"], timeout=6000)
    with pool.connection() as conn:
        table = conn.table(hbase_conf["table_name"])
        batch = table.batch()
        for row in table.scan():
            col_del = []
            for col in row[1]:
                strcol = col.decode("utf-8")
                for clean_col in hbase_conf["clean_column_pattern"]:
                    if clean_col.endswith('*'):
                        clean_col = clean_col[:-1]
                    if strcol.startswith(clean_col):
                        col_del.append(col)
            for col in col_del:
                print("delete:", row[0], col)
            batch.delete(row[0], columns=col_del)
            #break
        batch.send()
