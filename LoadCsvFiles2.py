import csv
import getopt
import sys
import os
import pyodbc


def usage():
    print("""\
    LoadCsvFiles 2.0: Handles performing updates instead of inserts where data is already present, identity inserts, \
    and UTF-8-BOM encoded files
    Usage: LoadCsvFiles [OPTIONS]
         -h --help                  Display this usage message
         -p --path                  Required. Path to CSV files
         -s --server                Required. Database server
         -d --database              Required. Database name
         -u --user                  Required. User name
         -w --password              Required. Password
    Example: LoadCsvFiles -p D:\Projects\Indiana\Dashboards-Plugin-EWS\Database\Data\Dashboard\DashboardTypes -s . \
-d IN_EdFi_Dashboard -u edfiPService -w edfiPService
    Example: LoadCsvFiles \
-p D:\Projects\Indiana\Ed-Fi-Dashboard\Etl\src\EdFi.Runtime\Reading\Queries\2.0\DashboardTypes \
-s . -d IN_EdFi_Dashboard -u edfiPService -w edfiPService
    """)


def get_args(opts):
    path = None
    server = None
    database = None
    user = None
    password = None
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage()
            sys.exit(3)
        if opt in ('-p', '--path'):
            path = arg
        if opt in ('-s', '--server'):
            server = arg
        if opt in ('-d', '--database'):
            database = arg
        if opt in ('-u', '--user'):
            user = arg
        if opt in ('-w', '--password'):
            password = arg
    return database, password, path, server, user


def get_files_from_path(path):
    # Get list of files from path
    name_list = os.listdir(path)
    # Join the path to the file names
    full_list = [os.path.join(path, i) for i in name_list]
    # Return the file list
    return full_list


def get_rows_from_file(file, file_encoding):
    # Open the file for reading
    with open(file, 'r', encoding=file_encoding) as fp:
        # Get a CSV reader
        reader = csv.reader(fp, delimiter=',', quotechar='"')
        # Get the headers from the file
        headers = next(reader, None)
        # Read the data from the CSV reader
        data_read = [row for row in reader]
    # Return the headers and data
    return headers, data_read


def get_table_name(file):
    # Split file path on \
    file_parts = file.split('\\')
    # Get the file name without the extension
    file, _ = os.path.splitext(file_parts[len(file_parts) - 1])
    # Split the file name because it is of type DashboardExport.MetricAction
    table_parts = file.split('.')
    # Get the last part of the file name as the table name
    table = table_parts[len(table_parts) - 1]
    # Return the table name
    return table


def run_data(table, headers, data, cursor):
    # Get schema information
    table_info = cursor.tables(table=table).fetchone()
    schema_name = table_info.table_schem

    # Determine if the table has an identity column
    has_identity = False
    for row in cursor.columns(table_info.table_name):
        if 'identity' in row.type_name:
            has_identity = True
            break

    # Get the primary key columns
    pk_columns = [row.column_name for row in
                  cursor.primaryKeys(table_info.table_name, table_info.table_cat, table_info.table_schem).fetchall()]

    try_later = []
    for row in data:
        # Turn headers and row data into key value paris and store in dictionary
        header_data_dict = dict(zip(headers, row))
        # Get a select statement to determine if row exists in table
        select = get_select_statement(schema_name, table, pk_columns, header_data_dict)
        print(select)
        # Run select statement
        result = cursor.execute(select).fetchone()
        # If row exists
        if result:
            # Get should update indicator and update statement
            # Should update indicator means the table contains rows (beyond the primary key) to be updated
            should_update, update = get_update_statement(headers, header_data_dict, schema_name, table, pk_columns)
            # If should update
            if should_update:
                print(update)
                # Try the update
                try:
                    cursor.execute(update)
                except pyodbc.IntegrityError:
                    # If it fails, supporting (foreign key) rows may not have been loaded yet,
                    # save it to try again later
                    try_later.append(update)
            # Else show there was nothing to update
            else:
                print('Nothing to update: ' + update)
        # Else row does not exist
        else:
            # If table has identity turn on identity insert
            if has_identity:
                cursor.execute('SET IDENTITY_INSERT ' + schema_name + '.' + table + ' ON')
            # Get the insert statement
            insert = get_insert_statement(headers, row, schema_name, table)
            print(insert)
            # Try the insert
            try:
                cursor.execute(insert)
            except pyodbc.IntegrityError:
                # If it fails, supporting (foreign key) rows may not have been loaded yet,
                # save it to try again later
                try_later.append(insert)
            finally:
                # If table has identity turn off identity insert
                if has_identity:
                    cursor.execute('SET IDENTITY_INSERT ' + schema_name + '.' + table + ' OFF')

    # For each try later statement
    for statement in try_later:
        print('Trying again: ' + statement)
        # Try it again
        try:
            cursor.execute(statement)
            # If it succeeds then remove it from the list
            try_later.remove(statement)
        except pyodbc.IntegrityError as e:
            # If it fails, print a fail statement
            print("Statement Fail:")
            print(e)

    # Return any statements that are still failing
    return try_later


def get_select_statement(schema_name, table, pk_columns, header_data_dict):
    # Build select statement to determine if row exists
    select = 'SELECT 1 FROM ' + schema_name + '.' + table + get_where_clause(pk_columns, header_data_dict)
    return select


def get_insert_statement(headers, row, schema_name, table):
    # Build insert statement
    statement = 'INSERT INTO ' + schema_name + '.' + table + ' (' + ", ".join(headers) + ') VALUES ('
    first = True
    for item in row:
        statement += ('' if first else ', ') + get_value(item)
        first = False
    statement += ')'
    return statement


def get_update_statement(headers, header_data_dict, schema_name, table, pk_columns):
    # Build update statement
    statement = 'UPDATE ' + schema_name + '.' + table + ' SET '
    first = True
    something_to_update = False
    for item in headers:
        if item in pk_columns:
            continue
        statement += ('' if first else ', ') + item + ' = ' + get_value(header_data_dict[item])
        first = False
        something_to_update = True
    statement += get_where_clause(pk_columns, header_data_dict)
    return something_to_update, statement


def get_where_clause(pk_columns, header_data_dict):
    # Build where clause
    where = ' WHERE '
    first = True
    for pk in pk_columns:
        where += ('' if first else ' AND ') + pk + ' = ' + get_value(header_data_dict[pk])
        first = False

    return where


def get_value(item):
    # Return null or value with quotes where appropriate
    if item == '':
        return 'null'

    value, parse = floatTryParse(item)
    if parse:
        return item
    else:
        return '\'' + item.replace('\'', '\'\'') + '\''


def floatTryParse(value):
    # Try to parse the value
    try:
        return float(value), True
    except ValueError:
        return value, False


def main(argv):
    print("Running main.")

    # Get command line options
    try:
        opts, args = getopt.getopt(argv, 'hp:s:d:u:w:', ['help', 'path=', 'server=', 'database=', 'user=', 'password='])
    except getopt.GetoptError:
        usage()
        sys.exit(1)

    # If options are empty exit
    if not len(opts):
        usage()
        sys.exit(2)

    # Get arguments
    database, password, path, server, user = get_args(opts)

    # If arguments are missing, show usage and exit
    if path is None or server is None or database is None or user is None or password is None:
        usage()
        sys.exit(4)

    # Get files from path
    print("Finding files in " + path)
    files = get_files_from_path(path)

    # For each file, if it is a csv file, get rows
    data_dictionary = {}
    for file in [file for file in files if file.endswith('.csv')]:
        print(file)
        # Try getting rows from the file as utf
        try:
            headers, data = get_rows_from_file(file, 'utf-8-sig')
        except UnicodeDecodeError:
            # Try getting rows from the file as cp1252 (common on windows)
            headers, data = get_rows_from_file(file, 'cp1252')
        print(headers)
        print(data)

        # Get table name
        table_name = get_table_name(file)

        # Load headers and rows into dictionary
        data_dictionary[table_name] = headers, data

    # Build the connection string
    connection_string = 'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + \
                        user + ';PWD=' + password
    print(connection_string)

    # Connect to the database
    connection = pyodbc.connect(connection_string, autocommit=False)
    cursor = connection.cursor()

    # Run statements for Metric and Metric Variant first
    table_name = 'Metric'
    headers, data = data_dictionary[table_name]
    failed_statements = run_data(table_name, headers, data, cursor)
    del data_dictionary[table_name]

    table_name = 'MetricVariant'
    headers, data = data_dictionary[table_name]
    failed_statements.append(run_data(table_name, headers, data, cursor))
    del data_dictionary[table_name]

    # Run the rest of the data
    for key in data_dictionary:
        headers, data = data_dictionary[key]
        failed_statements.append(run_data(key, headers, data, cursor))

    # Commit the changes and close the connection
    connection.commit()
    connection.close()

    # If there are still any failed statements then show them
    if failed_statements:
        print("At lease one statement failed.")
        for statement in failed_statements:
            print(statement)


if __name__ == '__main__':
    main(sys.argv[1:])
