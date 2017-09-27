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
    """)


def get_files_from_path(path):
    name_list = os.listdir(path)
    full_list = [os.path.join(path, i) for i in name_list]
    return full_list


def get_rows_from_file(file):
    with open(file, 'r', encoding='utf-8-sig') as fp:
        reader = csv.reader(fp, delimiter=',', quotechar='"')
        headers = next(reader, None)
        data_read = [row for row in reader]
    return headers, data_read


def get_table_name(file):
    file_parts = file.split('\\')
    file, file_ext = os.path.splitext(file_parts[len(file_parts) - 1])
    table_parts = file.split('.')
    table = table_parts[len(table_parts) - 1]
    return table


def run_data(table, headers, data, cursor):
    table_info = cursor.tables(table=table).fetchone()
    has_identity = False
    for row in cursor.columns(table_info.table_name):
        if 'identity' in row.type_name:
            has_identity = True
            break
    pk_columns = []
    for row in cursor.primaryKeys(table_info.table_name, table_info.table_cat, table_info.table_schem).fetchall():
        pk_columns.append(row.column_name)

    try_later = []
    for row in data:
        header_data_dict = dict(zip(headers, row))
        select = get_select_statement(table, pk_columns, header_data_dict)
        # print(select)
        result = cursor.execute(select).fetchone()
        if result:
            should_update, update = get_update_statement(headers, header_data_dict, table, pk_columns)
            if should_update:
                print(update)
                try:
                    cursor.execute(update)
                except pyodbc.IntegrityError:
                    try_later.append(update)
            else:
                print('Nothing to update: ' + update)
        else:
            insert = get_insert_statement(headers, row, table, has_identity)
            print(insert)
            cursor.execute(insert)

    for statement in try_later:
        print('Trying again: ' + statement)
        cursor.execute(statement)


def get_select_statement(table, pk_columns, header_data_dict):
    select = 'SELECT 1 FROM metric.' + table + get_where_clause(pk_columns, header_data_dict)
    return select


def get_insert_statement(headers, row, table, has_identity):
    statement = ''
    if has_identity:
        statement += 'SET IDENTITY_INSERT metric.' + table + ' ON; '
    statement += 'INSERT INTO metric.' + table + ' (' + ", ".join(headers) + ') VALUES ('
    first = True
    for item in row:
        statement += ('' if first else ', ') + get_value(item)
        first = False
    statement += ')'
    if has_identity:
        statement += 'SET IDENTITY_INSERT metric.' + table + ' OFF; '
    return statement


def get_update_statement(headers, header_data_dict, table, pk_columns):
    statement = 'UPDATE metric.' + table + ' SET '
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
    where = ' WHERE '
    first = True
    for pk in pk_columns:
        where += ('' if first else ' AND ') + pk + ' = ' + get_value(header_data_dict[pk])
        first = False

    return where


def get_value(item):
    if item == '':
        return 'null'

    value, parse = floatTryParse(item)
    if parse:
        return item
    else:
        return '\'' + item.replace('\'', '\'\'') + '\''


def floatTryParse(value):
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

    if path is None or server is None or database is None or user is None or password is None:
        usage()
        sys.exit(4)

    # Get files from path
    print("Finding files in " + path)
    files = get_files_from_path(path)

    # For each file get rows
    data_dictionary = {}
    for file in [file for file in files if file.endswith('.csv')]:
        print(file)
        headers, data = get_rows_from_file(file)
        print(headers)
        print(data)

        # Load headers and rows into dictionary
        table = get_table_name(file)
        data_dictionary[table] = headers, data

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
    run_data(table_name, headers, data, cursor)
    del data_dictionary[table_name]

    table_name = 'MetricVariant'
    headers, data = data_dictionary[table_name]
    run_data(table_name, headers, data, cursor)
    del data_dictionary[table_name]

    # Run the rest of the data
    for key in data_dictionary:
        headers, data = data_dictionary[key]
        run_data(key, headers, data, cursor)

    connection.commit()


if __name__ == '__main__':
    main(sys.argv[1:])
