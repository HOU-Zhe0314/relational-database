import csv
import tabulate

import DataTableExceptions
import CSVCatalog




max_rows_to_print = 10


class CSVTable:
    __catalog__ = CSVCatalog.CSVCatalog()

    def __init__(self, t_name, load=True):
        """
        Constructor.

        :param t_name: Name for table.
        :param load: Load data from a CSV file. If load=False, this is a derived table and engine will
            add rows instead of loading from file.
        """

        self.__table_name__ = t_name

        # Holds loaded metadata from the catalog.
        self.__description__ = None
        if load:
            self.__load_info__()  # Load metadata, stored in self.__description__
            self.__rows__ = []
            self.__load__()  # Load rows from the CSV file.

        else:
            self.__file_name__ = "DERIVED"

    def __load_info__(self):
        """
        Loads metadata from catalog and sets __description__ to hold the information.

        :return:Nothing
        """

        cat = CSVCatalog.CSVCatalog()
        t = cat.get_table(self.__table_name__)  # class TableDefinition
        self.__description__ = t

    def __get_file_name__(self):
        """
        Gets the file name from the description

        :return: string containing the file name
        """
        return self.__description__.file_name

    def __add_row__(self, row):
        """
        Adds a row to the table definition self.rows.
        Update indexes.

        :param row: The row to be added
        :return: Returns nothing
        """
        self.__rows__.append(row)
        defined_indexes = self.__description__.indexes

        for index in defined_indexes:

            name = index.index_name

            key_string = self.__get_key__(index, row)  # returns a string of the key that is the concatentated version for the index

            if key_string in self.__keys_added__:  # if key index already exists, row must be added to the list of rows
                self.__indexes__[name][key_string].append(row)
            else:
                self.__indexes__[name][key_string] = []
                self.__indexes__[name][key_string].append(row)
                self.__keys_added__.append(key_string)
        return

    def __get_key__(self, index, row):
        """
        Gets the key (a string) for the row based off the index columns.

        :param index: the index that we are creating the key for
        :param row: the row we are creating the key with, will also work for a template because a template is
                essentially a shortened row
        :return: a string which is the key of that row
        """

        key = []
        column_names = index.column_names
        for i in range(len(column_names)):
            key.append(row[column_names[i]])

        kstring = "_".join(key)
        return kstring

    def __load__(self):
        """
        Load rows from a file into a CSVTable object.
        Update indexes.

        :return: Nothing
        """
        self.__indexes__ = {}  # initialized indexes dictionary
        given_indexes = self.__description__.indexes
        self.__keys_added__ = []
        for index in given_indexes:
            self.__indexes__[index.index_name] = {}  # creates a dictionary for all the index:row values to go in

        try:
            fn = self.__get_file_name__()
            with open(fn, "r") as csvfile:

                reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')

                # Get the names of the columns defined for this table from the metadata.
                column_names = self.__get_column_names__()

                # Loop through each line (each dictionary to be precise) in the input file.
                for r in reader:
                    # Only add the defined columns into the in-memory table.
                    # The CSV file may contain columns that are not relevant to the definition.
                    projected_r = self.project([r], column_names)[0]

                    self.__add_row__(projected_r)

        except IOError as e:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_file,
                message="Could not read file = " + fn)

    def __get_column_names__(self):
        """
        Retrieves the column names from the table description.

        :return: a list with the column names
        """
        column_names = []
        column_list = self.__description__.columns
        for column in column_list:
            column_names.append(column.column_name)
        return column_names

    def project(self, rows, fields):
        """
        Performs the project. Returns a new table with only the requested columns.
        Example: if fields is [playerID, teamID]
            the project would return a table equivalent to "SELECT playerID, teamID FROM tablename"

        :param fields: A list of column names.
        :return: A new table derived from this table by PROJECT on the specified column names. (A list of dicts, every dict
        contains the values for the projecting fields)
        """
        try:
            # If there is not project clause, return the base table
            if fields is None:
                return rows
            else:
                result = []
                for r in rows:
                    tmp = {}
                    for j in range(0, len(fields)):
                        v = r[fields[j]]
                        tmp[fields[j]] = v
                    else:
                        result.append(tmp)

                return result

        # If the requested field not in rows.
        except KeyError as ke:
            raise DataTableExceptions.DataTableException(-2, "Invalid field in project")

    def __get_access_path__(self, fields):
        """
        i. Figures out if there is an index that can be used
        ii. If multiple indexes can be used , selects the most selective index
        Returns best index matching the set of keys in the template. Best is defined as the most selective index, i.e.
        the one with the most distinct index entries.

        An index name is of the form "colname1_colname2_coluname3" The index matches if the
        template references the columns in the index name. The template may have additional columns, but must contain
        all of the columns in the index definition.

        :param fields: Query template.
        :return: Two values, the index and the count, or None and None
        """

        if fields == []:
            print("Error! Empty fields")
            return
        # check for validity of indexes
        fields_set = set(fields)
        indexes = self.__description__.indexes

        if indexes is None:
            print("Error! Empty indexes")
        count = -1
        best_idx = None

        for idx in indexes:
            columns = idx.column_names
            s = set(columns)
            if s.issubset(fields_set):
                tmp = len(s)
                if tmp > count:
                    best_idx = idx
                    count = tmp

        return best_idx, count

    def matches_template(self, row, t):
        """
        A helper function that returns True if the row matches the template.

        :param row: A single dictionary representing a row in the table.
        :param t: A template as a dictionary
        :return: True if the row matches the template, False if not
        """

        # if there is no where clause.
        if t is None:
            return True

        try:
            c_names = list(t.keys())
            for n in c_names:
                if row[n] != t[n]:
                    return False
            else:
                return True

        except Exception as e:
            raise (e)

    def __find_by_template_scan__(self, t, fields=None):
        """
        Returns a new, derived table containing rows that match the template and the requested fields if any.
        Returns all rows if template is None and all columns if fields is None.

        :param t: The template representing a select predicate.
        :param fields: The list of fields (project fields)
        :return: New table (CSVTable obj) containing the result of the select and project.
        """

        if fields == []:
            fields = None

        result = []
        for r in self.__rows__:
            if self.matches_template(r, t):
                new_r = self.project([r], fields)[0]
                result.append(new_r)

        final_table = self.__table_from_rows__(table_name="scanned_table", rows=result)
        return final_table

    def __find_by_template_index__(self, t, idx_name, fields=None):
        """
        Find by template using a selected index.

        An example of an index is:
         {"TeamID": {"BOS":[list of dictionary rows with BOS]}, {"CL1":[list of dictionary rows with CL1]}}

        An index allows us to select rows much faster.

        :param t: Template representing a where clause/
        :param idx_name: Name of index to use.
        :param fields: Fields to return. #deciding not to push
        :return: New table (CSVTable obj) containing the result of the select and project.
        """

        if fields == []:
            fields = None

        if t == {}:
            return self.__find_by_template_scan__(t, fields)

        dic = self.__indexes__[idx_name]

        # create key string for the template
        idx_column_names = self.__description__.get_index(idx_name)[0].column_names
        key = ""
        for name in idx_column_names:
            key += t[name] + "_"
        else:
            key = key[:-1]

        rows = dic[key]
        new_table = self.__table_from_rows__(table_name="new_table", rows=rows)
        # print(new_table)

        result = []
        for r in new_table.__rows__:
            if new_table.matches_template(r, t):
                new_r = new_table.project([r], fields)[0]
                result.append(new_r)

        final_table = new_table.__table_from_rows__(table_name="final_table", rows=result)
        return final_table

    def __find_by_template__(self, template, fields=None, limit=None, offset=None):
        """
        # 1. Validate the template values relative to the defined columns.
        # 2. Determine if there is an applicable index, and call __find_by_template_index__ if one exists.
        # 3. Call __find_by_template_scan__ if not applicable index.

        :param template: Dictionary. The template that you search by
        :param fields: Fields that you want to return for the table
        :param limit: limit is not supported
        :param offset: offset is not supported
        :return: returns new list of rows that have the template and the fields applied
        """

        try:
            indexes = self.__indexes__
        except:
            indexes = None

        if indexes is None or indexes == {}:
            result_rows = self.__find_by_template_scan__(template, fields)

        else:
            result_index, count = self.__get_access_path__(template)
            if result_index is not None:

                #result_rows = self.__find_by_template_index__(template, result_index, fields)
                result_rows = self.__find_by_template_index__(template, result_index.index_name, fields)
            else:
                result_rows = self.__find_by_template_scan__(template, fields)

        return result_rows

    def dumb_join(self, right_r, on_fields, where_template=None, project_fields=None):
        """
        A 'dumb' JOIN on two CSV Tables. Support equi-join only on a list of common
        columns names.

        No optimizations and is just straightforward iteration.

        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: CSVTable object that is the joined and filtered rows
        """
        left_r = self
        left_rows = left_r.__get_row_list__()
        right_rows = right_r.__get_row_list__()
        result_rows = []

        left_rows_processed = 0
        for lr in left_rows:
            on_template = self.__get_on_template__(lr, on_fields)
            for rr in right_rows:
                if self.matches_template(rr, on_template):
                    new_r = {**lr, **rr}
                    result_rows.append(new_r)
            left_rows_processed += 1
            if left_rows_processed % 1000 == 0:
                print("Processed", left_rows_processed, "left rows.")

        join_result = self.__table_from_rows__("JOIN:" + left_r.__table_name__ + ":" + right_r.__table_name__, result_rows)
        result = join_result.__find_by_template__(template=where_template, fields=project_fields)  # join table won't have indexes so it will use template_scan
        final_table = self.__table_from_rows__("Filtered JOIN(" + self.__table_name__ + "," + right_r.__table_name__ + ")", result)
        return final_table

    def __smart_join__(self, right_r, on_fields, where_template=None, project_fields=None):
        """
        A JOIN on two CSV Tables. Support equi-join only on a list of common
        columns names.

        If no optimizations are possible, do a simple nested loop join and then apply where_clause and project to result

        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: A CSVTable. List of dictionary elements, each representing a row.
        """

        idx1, count1 = self.__get_access_path__(on_fields)
        idx2, count2 = right_r.__get_access_path__(on_fields)
        #print(count1, count2)


        # scenario 1: no indexes available in both tables, rewrite it as a more efficient querry
        if count1 == -1 and count2 == -1:
            print("no optimization by indexing")
            template_l = self.__get_sub_where_template__(where_template)
            left_table = self.__find_by_template__(template=template_l)
            template_r = right_r.__get_sub_where_template__(where_template)
            right_table = right_r.__find_by_template__(template=template_r)

            return left_table.dumb_join(right_table, on_fields, where_template, project_fields)

        # scenario 2: indexing available
        scan = self
        prob = right_r

        # check which table has a more efficient index
        if count1 > count2:
            scan = right_r
            prob = self

        #print(scan.__description__)

        result_rows = []
        for lr in scan.__get_row_list__():
            on_template = scan.__get_on_template__(lr, on_fields)
            result = prob.__find_by_template__(on_template)
            # print(result.__rows__)
            for rr in result.__rows__:
                new_r = {**lr, **rr}
                result_rows.append(new_r)

        join_result = scan.__table_from_rows__("JOIN:" + scan.__table_name__ + ":" + right_r.__table_name__,
                                               result_rows)
        result = join_result.__find_by_template__(template=where_template, fields=project_fields)
        final_table = scan.__table_from_rows__("Filtered JOIN(" + scan.__table_name__ + "," + right_r.__table_name__ + ")", result)
        return final_table

    def __get_sub_where_template__(self, where_template):
        """
        Gets the where template fields that are applicable to the table
        This means that someone could technically pass a template that references fields that do not exist in the table
        Not a real sql thing because sql would throw an error, here just implemented it for error checking.

        :param where_template:
        :return: where template dictionary
        """
        sub_template = {}
        table_columns = self.__get_column_names__()

        # Go through each key in the where template and see if it is a column in the table
        for key_name in where_template.keys():
            if key_name in table_columns:
                sub_template[key_name] = where_template[key_name]

        return sub_template

    def __get_on_template__(self, row, on_fields):
        """
        Gets the on clause as a template for an individual row to easily compare to other table

        :param row: the row that you are creating the template
        :param on_fields: list of fields to join ex: ['playerID', 'teamID']
        :return: template, a dict
        """
        template = {}
        for field in on_fields:
            value = row[field]
            template[field] = value

        return template

    def __get_row_list__(self):
        """
        Gets all rows of the table

        :return: List of row dictionaries
        """
        return self.__rows__

    def __table_from_rows__(self, table_name, rows):
        """
        Creates a new instance of CSVTable with a table name and rows passed through (from the join)

        :param table_name: String that is the name of the table
        :param rows: a list of dictionaries that contain row info for the table
        :return: the new table
        """
        new_table = CSVTable(table_name, False)
        new_table.__rows__ = rows
        new_table.__description__ = None

        return new_table

    def __str__(self):
        data = self.__rows__
        header = data[0].keys()

        # only print the first 100 rows
        rows = [x.values() for x in data[:100]]
        return tabulate.tabulate(rows, header, tablefmt='grid')

    def insert(self, r):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Insert not implemented"
        )

    def delete(self, t):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Delete not implemented"
        )

    def update(self, t, change_values):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Updated not implemented"
        )




