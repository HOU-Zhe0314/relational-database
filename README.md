# relational-database

create.sql: SQL statements to create tables and constraints for the catalog.



CSVCatalog: Creates the methods that interact with the SQL database to store the metadata tables

In CSVCatalog there are 4 classes created: ColumnDefinition, IndexDefinition, TableDefinition, and CSVCatalog.
CSV Catalog is the class the creates, drops, and loads a table.
A table definition is defined by file name and columns and indexes on those columns. These are created and column and index objects in ColumnDefiniton and IndexDefiniton.



CSVTable: the file that loads the metadata and csvfiles for specific tables. It is where the joins are created and acts more like a traditional MySQL workbench where we can access rows of data based off certain fields and join tables together. Find by template both via an index and a table scan has been implemented as well. 



DataTableExceptions: a file that raises specific exceptions.



unit_test_catalog.py: The test file for CSVCatalog.
unit_test_csv_table.py: A test file for CSVTable.