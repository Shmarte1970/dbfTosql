import dbf
table = dbf.Table("empresa.dbf")
table.open()
print(table.structure())