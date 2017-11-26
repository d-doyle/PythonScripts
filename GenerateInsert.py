import getopt
import os
import pyodbc
import sys


def usage():
    print("""\
    GenerateInsert: Generates insert statements for the specified file
    Usage: GenerateInsert [OPTIONS]
         -h --help                  Display this usage message
         -p --path                  Required. Path to CSV files
         -s --server                Required. Database server
         -d --database              Required. Database name
         -u --user                  Required. User name
         -w --password              Required. Password
         -t --table                 Required. Table name
    Example: GenerateInsert -p D:\Projects\Indiana\Ed-Fi-Dashboard\Database\ -s . -d IN_EdFi_Dashboard -u edfiPService \
    -w edfiPService -t metric.MetadataList
    """)


def get_args(opts):
    path = None
    server = None
    database = None
    user = None
    password = None
    table = None
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
        if opt in ('-t', '--table'):
            table = arg
    return database, password, path, server, table, user


def get_schema_and_table_name(table):
    values = table.split('.')
    schema, table_name = values[0], values[1]
    print(schema)
    print(table_name)
    return schema, table_name


def get_has_identity(cursor, table):
    # Get has identity sql for table
    identity_statement = 'SELECT OBJECTPROPERTY(OBJECT_ID(\'' + table + '\'), \'TableHasIdentity\') AS HasIdentity'
    # Show has identity sql
    print(identity_statement)
    # Execute has identity
    has_identity = cursor.execute(identity_statement).fetchone()
    # Print and return result
    return has_identity[0]


def write_inserts_to_file(columns, has_identity, path, results, table):
    # Open a file for the table insert scripts
    with open(os.path.join(path, 'InsertDataFor' + table + '.sql'), 'a', encoding='utf-8') as fp:
        # Clear the file
        fp.seek(0)
        fp.truncate()
        # If has identity use identity insert
        if has_identity:
            # Write turn on identity insert statement
            statement = 'SET IDENTITY_INSERT ' + table + ' ON;\n\n'
            print(statement)
            fp.write(statement)
        # For each row
        for row in results:
            # Get an insert statement and write it to the file
            insert_statement = get_insert_statement(columns, row, table)
            print(insert_statement)
            fp.write(insert_statement)
        # If has identity end identity insert
        if has_identity:
            # Write turn off identity insert statement
            statement = '\nSET IDENTITY_INSERT ' + table + ' OFF;\n'
            print(statement)
            fp.write(statement)


def get_insert_statement(columns, row, table):
    # Build insert statement with values and return it
    statement = 'INSERT INTO ' + table + ' (' + ", ".join(columns) + ') VALUES ('
    first = True
    for item in row:
        statement += ('' if first else ', ') + get_value(item)
        first = False
    statement += ')\n'
    return statement


def get_value(item):
    # If item is int or float, return it
    if isinstance(item, int) or isinstance(item, float):
        return str(item)

    # If item is none or empty return null
    if item is None or item == '':
        return 'null'

    # Return item in single quotes
    return '\'' + item.replace('\'', '\'\'') + '\''


def main(argv):
    print("Running main.")

    # Get command line options
    try:
        opts, args = getopt.getopt(argv, 'hp:s:d:u:w:t:',
                                   ['help', 'path=', 'server=', 'database=', 'user=', 'password=', 'target='])
    except getopt.GetoptError:
        usage()
        sys.exit(1)

    # If options are empty show usage and exit
    if not len(opts):
        usage()
        sys.exit(2)

    # Get command line arguments
    database, password, path, server, table, user = get_args(opts)

    # If missing required parameters show usage and exit
    if path is None or server is None or database is None or user is None or password is None or table is None:
        usage()
        sys.exit(4)

    # Build and show the connection string
    connection_string = 'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + \
                        user + ';PWD=' + password
    print(connection_string)

    # Connect to the database
    connection = pyodbc.connect(connection_string, autocommit=False)
    cursor = connection.cursor()

    # Get schema and table name
    schema, table_name = get_schema_and_table_name(table)

    # Select data and columns from the specified table
    sql = f"""
    DECLARE @sql nvarchar(4000) = 'SELECT * FROM ' + quotename('{schema}') + '.' + quotename('{table_name}')
    EXEC(@sql)
    """
    results = cursor.execute(sql).fetchall()
    columns = [column[0] for column in cursor.description]
    print(results)
    print(columns)

    # Determine if the table has an identity column
    has_identity = get_has_identity(cursor, table)
    print(has_identity)

    # Commit and transactions and close the connection
    connection.commit()
    connection.close()

    # Write inserts to file
    write_inserts_to_file(columns, has_identity, path, results, table)


if __name__ == '__main__':
    main(sys.argv[1:])
