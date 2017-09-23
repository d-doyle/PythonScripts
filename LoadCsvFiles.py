import csv
import getopt
import sys
import os
import pyodbc


def usage():
    print("""\
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
    with open(file, 'r') as fp:
        reader = csv.reader(fp, delimiter=',', quotechar='"')
        headers = next(reader, None)
        data_read = [row for row in reader]
    return headers, data_read


def get_statements(file, headers, data):
    file_parts = file.split('\\')
    file, file_ext = os.path.splitext(file_parts[len(file_parts) - 1])
    table_parts = file.split('.')
    table = table_parts[len(table_parts) - 1]

    statements = []
    for row in data:
        # noinspection PyTypeChecker
        statement = 'INSERT INTO metric.' + table + ' (' + ", ".join(headers) + ') VALUES ('
        first = True
        for item in row:
            if item == '':
                statement += 'null' if first else ', null'
                continue

            value, parse = floatTryParse(item)
            if parse:
                statement += ('' if first else ', ') + item
            else:
                statement += ('\'' if first else ', \'') + item + '\''
            first = False

        statement += ')'
        statements.append(statement)

    return table, statements


def floatTryParse(value):
    try:
        return float(value), True
    except ValueError:
        return value, False


def run_statements(statements, cursor):
    for statement in statements:
        print(statement)
        cursor.execute(statement)
        # row = cursor.fetchone()
        # print(row)


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
    print("Finding files for " + path)
    files = get_files_from_path(path)
    print(files)

    # For each file get rows
    statement_dictionary = {}
    for file in files:
        headers, data = get_rows_from_file(file)
        print(headers)
        print(data)

        # Load rows into dictionary
        table, statements = get_statements(file, headers, data)
        print(table)
        statement_dictionary[table] = statements

    # Build the connection string
    connection_string = 'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + \
                        user + ';PWD=' + password
    print(connection_string)

    # Connect to the database
    connection = pyodbc.connect(connection_string, autocommit=False)
    cursor = connection.cursor()

    # Run statements for Metric and Metric Variant first
    run_statements(statement_dictionary['Metric'], cursor)
    del statement_dictionary['Metric']
    run_statements(statement_dictionary['MetricVariant'], cursor)
    del statement_dictionary['MetricVariant']

    # Run the rest of the statements
    for key in statement_dictionary:
        run_statements(statement_dictionary[key], cursor)

    connection.commit()


if __name__ == '__main__':
    main(sys.argv[1:])
