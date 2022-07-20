import CSVTable
import CSVCatalog
import json
import csv


def drop_tables_for_prep():
    cat = CSVCatalog.CSVCatalog()
    cat.drop_table("people")
    cat.drop_table("batting")
    cat.drop_table("appearances")

# drop_tables_for_prep()


def create_lahman_tables():
    cat = CSVCatalog.CSVCatalog()
    cat.create_table("people", "/Users/hz/Downloads/NewPeople.csv")
    cat.create_table("batting", "/Users/hz/Downloads/NewBatting.csv")
    cat.create_table("appearances", "/Users/hz/Downloads/NewAppearance.csv")

# create_lahman_tables()


def update_people_columns():
    cat = CSVCatalog.CSVCatalog()

    column_lst = ["playerID","birthYear", "birthMonth","birthDay", "birthCountry", "birthState",
                  "birthCity", "deathYear", "deathMonth", "deathDay", "deathCountry", "deathState",
                  "deathCity", "nameFirst", "nameLast", "nameGiven", "weight", "height", "bats",
                  "throws", "debut", "finalGame", "retroID", "bbrefID"]
    t = cat.get_table("people")
    for col_name in column_lst:

        if col_name in ["playerID"]:
            c = CSVCatalog.ColumnDefinition(col_name, "text", not_null=True)
            t.add_column_definition(c)
        else:
            c = CSVCatalog.ColumnDefinition(col_name, "text", not_null=False)
            t.add_column_definition(c)

    # print("table is ", t)

# update_people_columns()


def update_appearances_columns():
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("appearances")

    column_lst = ["yearID", "teamID", "lgID", "playerID", "G_all", "GS",
                  "G_batting", "G_defense", "G_p","G_c", "G_1b", "G_2b",
                  "G_3b", "G_ss", "G_lf", "G_cf", "G_rf", "G_of", "G_dh", "G_ph", "G_pr"]

    for col_name in column_lst:

        if col_name in ["yearID", "teamID", "playerID"]:
            c = CSVCatalog.ColumnDefinition(col_name, "text", not_null=True)
            t.add_column_definition(c)
        else:
            c = CSVCatalog.ColumnDefinition(col_name, "text", not_null=False)
            t.add_column_definition(c)

    print("table is ", t)
    # for col_name in column_lst:

# update_appearances_columns()


def update_batting_columns():
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("batting")

    column_lst = ["playerID", "yearID", "stint", "teamID", "lgID", "G", "AB", "R", "H", "2B", "3B",
                  "HR", "RBI", "SB", "CS", "BB", "SO", "IBB", "HBP", "SH", "SF", "GIDP"]

    for col_name in column_lst:
        if col_name in ["playerID",  "yearID", "stint"]:
            c = CSVCatalog.ColumnDefinition(col_name, "text", not_null=True)
            t.add_column_definition(c)
        else:
            c = CSVCatalog.ColumnDefinition(col_name, "text", not_null=False)
            t.add_column_definition(c)

    print("table is ", t)

# update_batting_columns()


#Add primary key indexes for people, batting, and appearances in this test
def add_index_definitions():
    cat = CSVCatalog.CSVCatalog()

    # people table
    t = cat.get_table("people")
    idx = CSVCatalog.IndexDefinition("people_index", "PRIMARY", ["playerID"])
    t.define_index(idx.index_name, idx.column_names, idx.index_type)


    # appearances table
    t = cat.get_table("appearances")
    idx = CSVCatalog.IndexDefinition("appearance_index", "PRIMARY", ["yearID", "teamID", "playerID"])
    t.define_index(idx.index_name, idx.column_names, idx.index_type)

    # batting table
    t = cat.get_table("batting")
    idx = CSVCatalog.IndexDefinition("batting_index", "PRIMARY", ["playerID",  "yearID", "stint"])
    t.define_index(idx.index_name, idx.column_names, idx.index_type)

# add_index_definitions()


def test_load_info():
    table = CSVTable.CSVTable("people")
    # print(table.__description__.file_name)
    print(table.__description__)

# test_load_info()


def test_get_col_names():
    table = CSVTable.CSVTable("people")
    names = table.__get_column_names__()
    print(names)

# test_get_col_names()


def add_other_indexes():
    """

    People: nameLast, nameFirst
    Batting: teamID
    Appearances: None that are too important right now
    :return:
    """
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("people")
    idx = CSVCatalog.IndexDefinition("name_index", "INDEX", ["nameLast", "nameFirst"])
    t.define_index(idx.index_name, idx.column_names, idx.index_type)

    t = cat.get_table("batting")
    idx = CSVCatalog.IndexDefinition("team_index", "INDEX", ["teamID"])
    t.define_index(idx.index_name, idx.column_names, idx.index_type)
    print(t)

# add_other_indexes()


def load_test():
    batting_table = CSVTable.CSVTable("batting")
    print(batting_table)

# load_test()


def dumb_join_test():
    batting_table = CSVTable.CSVTable("batting")
    appearances_table = CSVTable.CSVTable("appearances")
    result = batting_table.dumb_join(appearances_table, ["playerID", "yearID"], {"playerID": "baxtemi01"},
                                     ["playerID", "yearID", "teamID", "AB", "H", "G_all", "G_batting"])
    print(result.__rows__)


# dumb_join_test()


def get_access_path_test():
    batting_table = CSVTable.CSVTable("batting")
    template = ["teamID", "playerID", "yearID","stint"]
    batting_table.__get_access_path__(template)
    index_result, count = batting_table.__get_access_path__(template)
    print(index_result)
    print(count)

# get_access_path_test()


def find_by_template_scan_test():
    batting_table = CSVTable.CSVTable("batting")
    template = {"playerID": "aardsda01", "lgID": "NL"}
    fields = ["playerID", "yearID"]
    result = batting_table.__find_by_template_scan__(template, fields)
    print("new table is ", result)

#find_by_template_scan_test()



def sub_where_template_test():
    batting_table = CSVTable.CSVTable("batting")
    template = {"playerID": "aardsda01", "lgID": "NL", "stint": "1", "wrong_column": "123"}
    result = batting_table.__get_sub_where_template__(template)
    print(result)

#sub_where_template_test()


def test_find_by_template_index():
    batting_table = CSVTable.CSVTable("batting")
    template = {"playerID": "aaronha01", "teamID": "ML1", "stint": "1"}
    fields = ["playerID", "teamID", "stint", "yearID", "lgID"]
    idx_name = "team_index"

    result = batting_table.__find_by_template_index__(template, idx_name, fields)
    print("table is ", result)

    print("------ check for the other index (composite index) ------")

    batting_table = CSVTable.CSVTable("batting")
    template = {"playerID": "aaronha01", "teamID": "ML1", "stint": "1", "yearID": "1954"}
    fields = ["playerID", "teamID", "stint", "yearID", "lgID"]
    idx_name = "batting_index"

    result = batting_table.__find_by_template_index__(template, idx_name, fields)
    print("table is ", result)

    print("------ empty template and empty fields ------")

    batting_table = CSVTable.CSVTable("batting")
    template = {}
    fields = None
    idx_name = "batting_index"

    result = batting_table.__find_by_template_index__(template, idx_name, fields)
    print("table is ", result)

# test_find_by_template_index()


def find_by_template_test():
    batting_table = CSVTable.CSVTable("batting")
    template = {"playerID": "aaronha01", "teamID": "ML1", "stint": "1"}
    fields = ["playerID", "teamID", "stint", "yearID", "lgID"]

    result = batting_table.__find_by_template__(template, fields)
    print("table is ", result)

#find_by_template_test()


def smart_join_test():
    batting_table = CSVTable.CSVTable("batting")
    appearances_table = CSVTable.CSVTable("appearances")

    print("--------------- indexing available ---------------")
    res = batting_table.__smart_join__(appearances_table, ["teamID", "playerID", "yearID"], {"playerID": "baxtemi01"},
                                 ["playerID", "yearID", "teamID", "AB", "H", "G_all", "G_batting"])

    print("table is ", res.__rows__)

    print("--------------- indexing unavailable ---------------")
    res = batting_table.__smart_join__(appearances_table, ["playerID", "yearID"], {"playerID": "baxtemi01"},
                                          ["playerID", "yearID", "teamID", "AB", "H", "G_all", "G_batting"])
    print("table is ", res.__rows__)

# smart_join_test()
