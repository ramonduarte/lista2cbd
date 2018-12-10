#%%
import csv
import heapq
from typing import List
from datetime import datetime


with open('consulta_cand_2018/consulta_cand_2018_BR.csv', encoding="latin-1") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    csv_info = [row[0].replace('"', '').split(';') for row in csv_reader]
    csv_fields = csv_info.pop(0)



#%%
def get_column(fieldname: str, header: List[str], table: List[list]) -> List[str]:
    index = header.index(fieldname)
    column = [x[index] for x in table]
    return column



#%%
class DBFile(object):
    struct = ""
    schema = ""

    def __init__(self, schema_file: str):
        schema_str = ""
        now = datetime.now().strftime("%Y%m%d%H%M%S")
        try:
            f = open(file=schema_file, mode="r", encoding="latin-1")
            file_content = f.readlines()
            if not file_content:
                raise OSError(schema_file + " was empty.")

            schema_str += "\n".join([
                file_content[:2]
                + ["Modification date: {}".format(now)]
                + file_content[-1]])
            f.close()
        except OSError:
            schema_str = "File structure: {}\n" \
            "Creation date: {}\n" \
            "Modification date: {}\n" \
            "Schema: {}".format(self.struct, now, now, self.schema)
        finally:
            f = open(file=schema_file, mode="w", encoding="latin-1")
            f.write(schema_str)
            f.close()

    def select(self, statement):
        raise NotImplementedError
    
    def insert(self, statement):
        raise NotImplementedError
    
    def delete(self, statement):
        raise NotImplementedError
    
    def parse(self, statement):
        raise NotImplementedError
    

class HeapDBFile(DBFile):
    heap = []
    struct = "Heap"

    def __init__(self, filename: str, schema_file: str):
        with open(filename, mode="r", encoding="latin-1") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            csv_info = [row[0].replace('"', '').split(';') for row in csv_reader]
            csv_fields = csv_info.pop(0)

            self.schema = ":{}({});".join(csv_fields)

            super().__init__(schema_file)

new_heap = HeapDBFile("consulta_cand_2018/consulta_cand_2018_BR.csv", "test.txt")