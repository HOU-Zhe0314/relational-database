import pymysql
import json



def run_q(cnx, q, args, fetch=False):
    """
    Method to run queries on your AWS MySQL Database.

    :param cnx: Connection to database
    :param q: The query string
    :param args: Any arguments passed
    :param fetch: Whether the query needs to return data
    :return: Result from query, if applicable
    """
    cursor = cnx.cursor()
    print("Q = ", q)
    cursor.execute(q, args)
    if fetch:
        result = cursor.fetchall()
    else:
        result = None
    cnx.commit()
    return result


class ColumnDefinition:
    """
    A class defining a column.
    Represents a column definition in the CSV Catalog.

    """

    # Allowed types for a column, can be extended later.
    column_types = ("text", "number")

    def __init__(self, column_name, column_type="text", not_null=False):
        """

        :param column_name: Cannot be None.
        :param column_type: Must be one of valid column_types, defaults to text
        :param not_null: True or False, whether the column can have NULL fields or not.
        """

        if column_name == None:
            print("issue!!")
            raise ValueError('You must have a column name!!')
        else:
            self.column_name = column_name

        if column_type in self.column_types:
            self.column_type = column_type
        else:
            print("Issue!")
            raise ValueError('That column type is not accepted. Please try again.')

        if type(not_null) == type(True):
            self.not_null = not_null
        else:
            print("issue!")
            raise ValueError('The not_null column must be either True or False! Please try again.')

    def __str__(self):
        return json.dumps(self.to_json(), indent=2)

    def to_json(self):
        """
        :return: A JSON object, not a string, representing the column and it's properties.
        """
        result = {
            "column_name": self.column_name,
            "column_type": self.column_type,
            "not_null": self.not_null
        }
        return result


class IndexDefinition:
    """
    A class defining an index.
    Represents the definition of an index.

    """
    index_types = ("PRIMARY", "UNIQUE", "INDEX")

    def __init__(self, index_name, index_type, column_names):
        """
        :param index_name: Name for index. Must be unique name for table.
        :param index_type: Valid index type.
        """
        self.index_name = index_name
        if index_type not in IndexDefinition.index_types:
            raise ValueError("Not the right index type")
        if len(column_names) == 0:
            raise ValueError("Must have an associate column name")

        self.index_type = index_type

        # column_names must always be initialized in proper order because of how we load/create indexes
        self.column_names = column_names

    def __str__(self):
        return json.dumps(self.to_json(), indent=2)

    def to_json(self):
        result = {
            "index_name": self.index_name,
            "type": self.index_type,
            "columns": self.column_names
        }
        return result


class TableDefinition:
    """
    Represents the definition of a table in the CSVCatalog.
    (Represents metadata information about a CSVTable)

    """

    def __init__(self, t_name=None, csv_f=None, column_definitions=None, index_definitions=None, cnx=None,
                 load=False):

        """
        Constructor.

        :param t_name: Name of the table.
        :param csv_f: Full path to a CSV file holding the data.
        :param column_definitions: List of column definitions to use from file. Cannot contain invalid column name.
            May be just a subset of the columns.
        :param index_definitions: List of index definitions. Column names must be valid.
        :param cnx: Database connection to use. If None, create a default connection.
        :param load: Whether you are creating a new TableDefinition or loading a preexisting one.
        """
        self.cnx = cnx
        self.table_name = t_name
        self.file_name = csv_f
        self.columns = None
        self.indexes = None

        if not load:

            if t_name is None or csv_f is None:
                raise ValueError("Table and file must both have a name")


            self.file_name = csv_f
            self.save_core_definition()

            if column_definitions is not None:

                for c in column_definitions:
                    self.add_column_definition(c)

            if index_definitions is not None:
                for idx in index_definitions:
                    # self.define_index(idx)
                    self.define_index(idx.name, idx.column_names, idx.type)

        else:
            self.load_core_definition()  # load self.file_name
            self.load_columns()  # load self.columns
            self.load_indexes()  # load self.indexes

    def __str__(self):
        return json.dumps(self.to_json(), indent=2)

    def load_columns(self):
        """
        Method to query the metadata table and update self.columns with ColumnDefinitions stored

        :return: Nothing
        """
        # ************************ TO DO ***************************
        q = "select * from csvcolumns where table_name= %s"
        res = run_q(self.cnx, q, (self.table_name), fetch=True)
        for r in res:
            nn = True
            if r["not_null"] == 0:
                nn = False
            else:
                nn = True

            if self.columns is None:
                self.columns = []

            c = ColumnDefinition(r["column_name"], r["type"], nn)
            self.columns.append(c)

    def load_indexes(self):
        """
        Get the index information from the csvindexes table in AWS and create the necessary IndexDefinition

        :return: Nothing
        """

        q = "select * from csvindexes where table_name = %s "

        res = run_q(self.cnx, q, (self.table_name), fetch=True)

        if self.indexes is None:
            self.indexes = []
        dic = {}
        for entry in self.indexes:
            dic[entry["index_name"]] = entry

        for r in res:
            idx_name = r["index_name"]
            if idx_name in dic:
                idx = dic[idx_name]
                idx.column_names.append(r["column_name"])

            else:
                idx = IndexDefinition(idx_name, r["type"], [r["column_name"]])
                dic[idx_name] = idx

        if self.indexes is None:
            self.indexes = []

        for key in dic.keys():
            obj = dic[key]

            self.indexes.append(obj)
            order_count = len(obj.column_names)
            for i in range(order_count):
                q = "update csvindexes set index_order = if(index_name = %s and type = %s and column_name = %s, %s, index_order)"
                v = (obj.index_name, obj.index_type, obj.column_names[i], i)
                run_q(self.cnx, q, v)

    def load_core_definition(self):
        """
        Loads a TableDefinition by querying the metadata tables in AWS (load self.file_name)

        :return: Nothing
        """

        q = "select * from csvtables where table_name = %s"
        res = run_q(self.cnx, q, (self.table_name), fetch=True)
        # print(res)
        if len(res) == 1:
            res = res[0]
            # print("test {0}".format(res))
            self.file_name = res["path"]
        else:
            raise ValueError("No such table! You cannot load it!")

    def save_core_definition(self):
        """
        Insert key info of the new table into 'csvtables'

        :return: Nothing
        """
        print("Running save core definition")
        q = "insert into csvtables values(%s, %s)"
        result = run_q(self.cnx, q, (self.table_name, self.file_name), fetch=True)

    def add_column_definition(self, c):
        """
        Add a column definition to self.columns.
        Insert column info into 'csvcolumns'

        :param c: ColumnDefinition obj. New column. Cannot be duplicate or column not in the file.
        :return: None
        """
        #SQL will throw the error if table integrity is not kept and a column with a duplicate
        q = "insert into csvcolumns values(%s, %s, %s, %s)"
        result = run_q(self.cnx, q, (self.table_name, c.column_name, c.column_type, c.not_null), fetch=True)
        if self.columns is None:
            self.columns = []
        self.columns.append(c)

    def get_column(self, cn):
        """
        Returns a column of the current table so that it can be deleted from the columns list
        Helper method for drop_column_definition(self, cn).

        :param cn: name of the column you are trying to get.
        :return: Column if found or None
        """
        for col in self.columns:
            if col.column_name == cn:
                return col

        print("Column '" + cn + "' not found")
        return

    def drop_column_definition(self, cn):
        """
        Remove from definition and catalog tables.

        :param cn: Column name (string)
        :return: Returns nothing, removes from the columns list
        """
        column_to_drop = self.get_column(cn)
        if column_to_drop is not None:
            self.columns.remove(column_to_drop)
            self.drop_col_in_sql(cn)
            print("Column '" + cn + "' has been dropped!")

        return

    def drop_col_in_sql(self, cn):
        """
        Deletes the row in sql for the given column.

        :param cn: Column name (string)
        :return: Returns nothing, executes SQL query
        """
        # drop corresponding indexes
        q = "delete from csvindexes where column_name =  %s"
        v = (cn)
        res = run_q(self.cnx, q, v, fetch=True)

        # drop column
        q = "delete from csvcolumns where column_name =  %s"
        v = (cn)
        res = run_q(self.cnx, q, v, fetch=True)

    def to_json(self):
        """
        Helper function for __str__(self)

        :return: A JSON representation of the table and it's elements.
        """
        result = {
            "table_name": self.table_name,
            "file_name": self.file_name
        }

        if self.columns is not None:
            result['columns'] = []
            for c in self.columns:
                result['columns'].append(c.to_json())

        if self.indexes is not None:
            print("in the index if statement")
            result['indexes'] = []
            for idx in self.indexes:
                result['indexes'].append(idx.to_json())
        return result

    def save_index_definition(self, i_name, cols, type):
        """
        Performs the insert into the 'csvindexes' table using a query and the run_q method
        Is a helper method for define_index(self, index_name, columns, type="index") method below.

        :param i_name: name of the index
        :param cols: columns the index is defined on
        :param type: type of index
        :return: Does not return anything.
        """

        q = "insert into csvindexes (table_name, column_name, type, index_name, index_order) " + \
            " values(%s, %s, %s, %s, %s)"
        for i in range(0, len(cols)):
            #print("check " + cols[0])
            #v = (self.table_name, cols[i].column_name, type, i_name, str(i))
            # the most left column has the smallest index_order
            v = (self.table_name, cols[i], type, i_name, str(i))
            result = run_q(self.cnx, q, v, fetch=False)

    def define_index(self, index_name, columns, type="index"):
        """
        Insert index info into the 'csvindexes' table
        Define or replace and index definition.
        Update the self.indexes variable.

        :param index_name: Index name, must be unique within a table.
        :param columns: Valid list of columns.
        :param type: One of the valid index types.
        :return: Returns nothing
        """

        self.save_index_definition(index_name, columns, type)
        if self.indexes is None:
            self.indexes = []

        # check if there exists an index with the same name
        for obj in self.indexes:
            obj_index = obj.index_name
            if obj_index == index_name:
                for col in columns:
                    obj.column_names.append(col)
                return

        idx = IndexDefinition(index_name, type, columns)
        self.indexes.append(idx)

    def get_index(self, ind_name):
        """
        Gets the IndexDefinition matching the name, if exists.
        Prints "Index [ind_name] not found" and returns None if index is not found.

        :param ind_name: name of the index you are trying to get.
        :return: Returns the index list, if found
        """
        idx_lst = []
        for idx in self.indexes:
            if idx.index_name == ind_name:
                idx_lst.append(idx)

        if idx_lst != []:
            return idx_lst

        print("Index '" + ind_name + "' not found")
        return

    def drop_index(self, index_name):
        """
        Remove an index from the metadata and the list of indexes associated with the table

        :param index_name: Name of index to remove (str)
        :return: Returns nothing.
        """
        idx_lst = self.get_index(index_name)
        # column_to_drop = self.get_column(cn)
        if idx_lst is not None:
            for idx in idx_lst:
                self.indexes.remove(idx)

            self.drop_indx_in_sql(index_name)
            print("index '" + index_name + "' has been dropped!")

        return

    def drop_indx_in_sql(self, index_name):
        """
        Helper method for drop_index.
        Deletes the row for the given index in SQL.

        :param index_name: name of index to remove (str)
        :return: Returns nothing.
        """
        q = "DELETE FROM csvindexes WHERE table_name = '" + self.table_name + "' and index_name = '" + index_name + "'"
        result = run_q(self.cnx, q, None, fetch=True)

    def describe_table(self):
        """
        Simply wraps to_json()

        :return: JSON representation.
        """
        result = self.to_json()
        return result


class CSVCatalog:
    """
    A class to create, drop, and retrieve a table

    """

    def __init__(self, dbhost="database-4111.cpwlqqnivbtg.us-east-1.rds.amazonaws.com", dbport=3306,
                  dbuser="admin", dbpw="dbuserdbuser", db="CSVCatalog", debug_mode=None):
        self.cnx = pymysql.connect(
            host=dbhost,
            port=dbport,
            user=dbuser,
            password=dbpw,
            db=db,
            cursorclass=pymysql.cursors.DictCursor
        )

    def __str__(self):
        pass

    def create_table(self, table_name, file_name, column_definitions=None, primary_key_columns=None):
        """
        Create a TableDefinition object.

        :param table_name:
        :param file_name:
        :param column_definitions:
        :param primary_key_columns:
        :return: A TableDefinition object
        """

        result = TableDefinition(table_name, file_name, cnx=self.cnx, column_definitions=column_definitions)
        return result

    def drop_table(self, table_name):
        """
        Delete metadata of the table.

        :param table_name: table name you want to drop.
        :return: Nothing
        """

        q = "DELETE FROM csvtables WHERE table_name = '" + table_name + "'"
        result = run_q(self.cnx, q, None, fetch=True)
        print("Table '" + table_name + "' was dropped")

    def get_table(self, table_name):
        """
        Get a previously created table.

        :param table_name: Name of the table.
        :return: A table (Class TableDefinition)
        """
        result = TableDefinition(table_name, load=True, cnx=self.cnx)

        return result


