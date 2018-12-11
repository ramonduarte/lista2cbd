#%%
import csv
import heapq
import sqlparse
from typing import List
from datetime import datetime


with open('consulta_cand_2018/consulta_cand_2018_BR.csv',
          encoding="latin-1") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter='\n')
    csv_info = [row[0].replace('"', '').replace(",", " ").split(';')
                for row in csv_reader]
    csv_fields = csv_info.pop(0)



#%%



#%%
class HeapRecord(list):
    sort_parameters = []

    def __init__(self, list_of_objs: list, parameters: List[int] = [0]):
        self.sort_parameters = parameters
        super().__init__(list_of_objs)

    def __lt__(self, value):
        for field in self.sort_parameters:
            if self[field] < value[field]:
                return True
            elif self[field] > value[field]:
                return False
        else:
            return False

    def __gt__(self, value):
        for field in self.sort_parameters:
            if self[field] < value[field]:
                return False
            elif self[field] > value[field]:
                return True
        else:
            return False


class HeapTable(list):
    sort_parameters = []

    def __init__(self, list_of_objs: list, parameters: List[int] = [0]):
        list_of_objs = [HeapRecord(x, parameters) for x in list_of_objs]
        super().__init__(list_of_objs)



#%%
class DBFile(object):
    data = None
    header = None
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

            schema_str += "".join(
                file_content[:2]
                + ["Modification date: {}".format(now) + "\n"]
                + [file_content[-1]])
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

    def get_column(self, fieldname: str) -> List[str]:
        index = self.header.index(fieldname)
        column = [x[index] for x in self.data]
        return column


class HeapDBFile(DBFile):
    struct = "Heap"

    def __init__(self, filename: str, schema_file: str,
                 parameters: List[int] = [15]):
        with open(filename, mode="r", encoding="latin-1") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            self.data = HeapTable([HeapRecord([row[0].replace('"', '').split(';')
                                   for row in csv_reader])], parameters)[0]
            self.header = self.data.pop(0)
            self.schema = ":{}({});".join(self.header)

            super().__init__(schema_file)

    def parse(self, statement):
        ops = sqlparse.parse(statement)[0].tokens
        if str(ops[0]).lower() == "select":
            return self.select(ops[1:])
        if str(ops[0]).lower() == "insert":
            return self.insert(ops[1:])
        if str(ops[0]).lower() == "delete":
            return self.delete(ops[1:])
        
        return False

    def select(self, statement):
        columns = str(statement[0]).split(",")
        tables = str( (statement[2:] + [None])[0] )
        where = str( (statement[3:] + [None])[0] ).split(" and ")

        return zip(*[self.get_column(x) for x in self.header])



new_heap = HeapDBFile("consulta_cand_2018/consulta_cand_2018_BR.csv", "test.txt")
result = new_heap.parse("select shdnslkjdhn")
for x in result:
    for y in x:
        print(y, end=",")
    print("\n")
