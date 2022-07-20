import CSVCatalog
import json


def create_table_test():
    cat = CSVCatalog.CSVCatalog(
        dbhost="XXX",
        dbport=3306,
        dbuser="admin",
        dbpw="XXX",
        db="CSVCatalog")
    cat.create_table("test_table", "file_path_test.woo")

    t = cat.get_table("test_table")
    print("Table = ", t)
    #cat.drop_table("test_table")

#create_table_test()

def drop_table_test():
    cat = CSVCatalog.CSVCatalog(
        dbhost="XXX",
        dbport=3306,
        dbuser="admin",
        dbpw="XXX",
        db="CSVCatalog")
    # t = cat.get_table("test_table")
    # print("before dropping Table = ", t)

    cat.drop_table("test_table")
    # t = cat.get_table("test_table")
    # print("after dropping Table = ", t)

#drop_table_test()

def add_column_test():
    print("--- in add_column_test ---")
    cat = CSVCatalog.CSVCatalog(
        dbhost="XXX",
        dbport=3306,
        dbuser="admin",
        dbpw="XXX",
        db="CSVCatalog")
    t = cat.get_table("test_table")
    c = CSVCatalog.ColumnDefinition("first_col","text",not_null= False)
    t.add_column_definition(c)
    c = CSVCatalog.ColumnDefinition("random1", "number", not_null=False)
    t.add_column_definition(c)
    c = CSVCatalog.ColumnDefinition("random2", "number", not_null=False)
    t.add_column_definition(c)
    c = CSVCatalog.ColumnDefinition("random3", "number", not_null=False)
    t.add_column_definition(c)

    print("Table = ", t)
    print("------------------------")

#add_column_test()

def load_column_test():
    print("--- load_column_test ---")
    cat = CSVCatalog.CSVCatalog(
        dbhost="XXX",
        dbport=3306,
        dbuser="admin",
        dbpw="XXX",
        db="CSVCatalog")
    t = cat.get_table("test_table")
    print("Table = ", t)

    print("------------------ ---")

#load_column_test()


def column_name_failure_test():
    cat = CSVCatalog.CSVCatalog(
        dbhost="XXX",
        dbport=3306,
        dbuser="admin",
        dbpw="XXX",
        db="CSVCatalog")
    col = CSVCatalog.ColumnDefinition(None, "text", False)
    t = cat.get_table("test_table")
    t.add_column_definition(col)

#column_name_failure_test()


def column_type_failure_test():
    cat = CSVCatalog.CSVCatalog(
        dbhost="XXX",
        dbport=3306,
        dbuser="admin",
        dbpw="XXX",
        db="CSVCatalog")
    col = CSVCatalog.ColumnDefinition("bird", "canary", False)
    t = cat.get_table("test_table")
    t.add_column_definition(col)

#column_type_failure_test()


def column_not_null_failure_test():
    cat = CSVCatalog.CSVCatalog(
        dbhost="XXX",
        dbport=3306,
        dbuser="admin",
        dbpw="XXX",
        db="CSVCatalog")
    col = CSVCatalog.ColumnDefinition("name", "text", "happy")
    t = cat.get_table("test_table")
    t.add_column_definition(col)

#column_not_null_failure_test()


def add_index_test():
    # define index
    cat = CSVCatalog.CSVCatalog(
        dbhost="XXX",
        dbport=3306,
        dbuser="admin",
        dbpw="XXX",
        db="CSVCatalog")
    t = cat.get_table("test_table")

    idx = CSVCatalog.IndexDefinition("first_index", "INDEX", ["first_col"])
    t.define_index(idx.index_name,idx.column_names,idx.index_type)

    idx = CSVCatalog.IndexDefinition("second_index", "INDEX", ["random1"])
    t.define_index(idx.index_name, idx.column_names, idx.index_type)


    idx = CSVCatalog.IndexDefinition("second_index", "INDEX", ["random2","random3"])
    t.define_index(idx.index_name, idx.column_names, idx.index_type)

    #t = cat.get_table("test_table")
    print("Table = ", t)

#add_index_test()


def load_index_test():
    # define index
    cat = CSVCatalog.CSVCatalog(
        dbhost="XXX",
        dbport=3306,
        dbuser="admin",
        dbpw="XXX",
        db="CSVCatalog")
    t = cat.get_table("test_table")
    print(t)

#load_index_test()

def col_drop_test():
    print("--- drop_column_test ---")
    cat = CSVCatalog.CSVCatalog(
        dbhost="XXX",
        dbport=3306,
        dbuser="admin",
        dbpw="XXX",
        db="CSVCatalog")
    t = cat.get_table("test_table")
    t.drop_column_definition("first_col")

    t = cat.get_table("test_table")
    print("after dropping first_col, table= ", t)


    t.drop_column_definition("random2")
    t = cat.get_table("test_table")
    print("after dropping random2, table= ", t)
    print("----------------------")

#col_drop_test()

def index_drop_test():
    print("--- drop_index_test ---")
    cat = CSVCatalog.CSVCatalog(
        dbhost="XXX",
        dbport=3306,
        dbuser="admin",
        dbpw="XXX",
        db="CSVCatalog")
    t = cat.get_table("test_table")
    ## check for invalid input
    idx = t.get_index("error_index")
    idx = t.get_index("second_index")
    t.drop_index("second_index")
    print("table after dropping ", t)

#index_drop_test()

def describe_table_test():
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("test_table")
    desc = t.describe_table()

    print("DESCRIBE People = \n", json.dumps(desc, indent = 2))


#describe_table_test()

