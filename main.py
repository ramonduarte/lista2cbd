#%%
import csv
import heapq
import sqlparse
from typing import List
from datetime import datetime
from timeit import default_timer as timer


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
        heapq.heapify(list_of_objs)
        super().__init__(list_of_objs)

    def append(self, value):
        return heapq.heappush(self, value)

    def pop(self, index=-1):
        if index == 0:
            return super().pop(0)
        return heapq.heappop(self)


#%%
class DBFile(object):
    data = None
    header = None
    schema_map = {}
    struct = ""
    schema = ""
    record_file = ""

    def __init__(self, schema_file: str):
        try:
            schema_str = ""
            now = datetime.now().strftime("%Y%m%d%H%M%S")
            f = open(file=schema_file, mode="r", encoding="latin-1")
            file_content = f.readlines()
            if not file_content:
                raise OSError(schema_file + " was empty.")

            schema_str += "".join(
                file_content[:2]
                + ["Modification date: {}".format(now) + "\n"]
                + [file_content[-1]])
            f.close()

            for x in file_content[-1].replace("Schema: ",
                                              "").replace(")", "").split(";"):
                if x:
                    self.schema_map[x.split(":")[0]] = int(x.split(":")[1].split("(")[1])
        except OSError:
            schema_str = "File structure: {}\n" \
            "Creation date: {}\n" \
            "Modification date: {}\n" \
            "Schema: {}".format(self.struct, now, now, self.schema)
        finally:
            f = open(file=schema_file, mode="w", encoding="latin-1")
            f.write(schema_str)
            f.close()


        try:
            f = open(file=self.record_file, mode="w", encoding="latin-1")
            for x in self.data:
                f.write(";".join(x) + "\n")
            f.close()
        except OSError:
            pass


    def select(self, statement):
        raise NotImplementedError
    
    def insert(self, statement):
        raise NotImplementedError
    
    def delete(self, statement):
        raise NotImplementedError
    
    def parse(self, statement):
        raise NotImplementedError

    def get_column(self, fieldname: str) -> List[str]:
        index = self.header.index(fieldname.strip())
        column = [x[index] for x in self.data]
        return column


class HeapDBFile(DBFile):
    struct = "Heap"
    record_file = "heapfile.csv"

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
        columns = str(statement[1]).split(",")
        tables = str( (statement[2:] + [None])[0] )
        where = str( (statement[3:] + [None])[0] ).lower().replace("where ", "").replace(";", "").split(" and ")
        for w in where:
            # TODO: parse "=", "in" and "between"
            break

        if columns[0] == '*':
            return zip(*[self.get_column(x) for x in self.header])
        else:
            return zip(*[self.get_column(x.upper()) for x in columns])

    def insert(self, statement):
        tables = str(statement[1])
        columns = str(statement[2]).upper().strip().replace("(", "").replace(")", "").split(",")
        values = str(statement[4]).upper().strip().replace("(", "").replace(")", "").split(",")

        assert len(columns) == len(values)
        new_record = [None] * len(self.header)
        for column in range(len(self.header)):
            if column in columns:
                i = self.header.index(column)
                j = columns.index(column)
                new_record[i] = values[j]
        
        self.data.append(HeapRecord(new_record, self.data.sort_parameters))

        return True

    def delete(self, statement):
        tables = str(statement[1])
        columns = str(statement[1]).split(",")
        where = str(statement[2]).split(" and ")
        # where = str( (statement[2:] + [None])[0] ).split(" and ")

        if columns[0] == '*':
            return zip(*[self.get_column(x) for x in self.header])
        else:
            return zip(*[self.get_column(x.upper()) for x in columns])


## HEAP TESTBED
# load heap and write to file
counter = 0
start = timer()
new_heap = HeapDBFile("consulta_cand_2018/consulta_cand_2018_BR.csv", "HeapHEAD.txt")
print(counter, timer() - start)

# SELECT statements
counter += 1
start = timer()
new_heap.parse("select * from table_name where SQ_CANDIDATO = 280000622172);")
print(counter, timer() - start)

counter += 1
start = timer()
new_heap.parse("select * from table_name where SQ_CANDIDATO in (280000624082, 280000624083, 280000624085, 280000624086, 280000625869, 280000625870, 280000629807, 280000629807, 280000629808, 280000629808;")
print(counter, timer() - start)

counter += 1
start = timer()
new_heap.parse("select * from table_name where SQ_CANDIDATO between 280000600000 and 280000700000;")
print(counter, timer() - start)

counter += 1
start = timer()
new_heap.parse("select * from table_name where NM_PARTIDO = PODEMOS and DS_ESTADO_CIVIL = CASADO(A);")
print(counter, timer() - start)


# INSERT statements
counter += 1
start = timer()
with open("insert1.txt") as f:
    sql = f.readlines()
for statement in sql:
    new_heap.parse(statement)
print(counter, timer() - start)

counter += 1
start = timer()
with open("insert10.txt") as f:
    sql = f.readlines()
for statement in sql:
    new_heap.parse(statement)
print(counter, timer() - start)

counter += 1
start = timer()
with open("insert10.txt") as f:
    sql = f.readlines()
for statement in sql:
    new_heap.parse(statement)
print(counter, timer() - start)

counter += 1
start = timer()
with open("insert10.txt") as f:
    sql = f.readlines()
for statement in sql:
    new_heap.parse(statement)
print(counter, timer() - start)

counter += 1
start = timer()
with open("insertall.txt") as f:
    sql = f.readlines()
for statement in sql:
    new_heap.parse(statement)
print(counter, timer() - start)


# DELETE statements
counter += 1
start = timer()
new_heap.parse("delete from table_name where SQ_CANDIDATO = 280000622172;")
print(counter, timer() - start)

counter += 1
start = timer()
new_heap.parse("delete from table_name where SQ_CANDIDATO in (280000624082, 280000624083, 280000624085, 280000624086, 280000625869, 280000625870, 280000629807, 280000629807, 280000629808, 280000629808);")
print(counter, timer() - start)

counter += 1
start = timer()
new_heap.parse("delete from table_name where SQ_CANDIDATO between 280000600000 and 280000700000;")
print(counter, timer() - start)

counter += 1
start = timer()
new_heap.parse("delete from table_name where NM_PARTIDO = PODEMOS and DS_ESTADO_CIVIL = CASADO(A);")
print(counter, timer() - start)
