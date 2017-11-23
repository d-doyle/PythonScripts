import getopt
import glob
import sys
import pyodbc
import os

level = 'warning'


class ScreenColors:
    HEADER = '\033[95m'
    OK_BLUE = '\033[94m'
    OK_GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    END_C = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def usage():
    print("""\
    RunSqlFiles find and run sql files in the given path
    Usage: LoadCsvFiles [OPTIONS]
         -h --help                  Display this usage message
         -p --path                  Required. Path to SQL files
         -s --server                Required. Database server
         -d --database prefix       Required. Database prefix
         -u --user                  Required. User name
         -w --password              Required. Password
    Example: RunSqlFiles -p D:\Projects\DelawareDOE\Dashboards-Plugin-EWS\Database -s . \
-d EdFi_ -u edfiPService -w edfiPService
    """)


def get_and_run_files_from_path(database, password, path, server, user):
    # Get files from path
    print('Finding files in ' + path)
    security_files = get_security_files_from_path(path)
    other_files = get_other_files_from_path(path)
    # Show files found
    for file in security_files + other_files:
        print(file)

    # Build and show the connection string
    connection_string = 'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + \
                        user + ';PWD=' + password
    print(connection_string)

    # Show file count and ask user if continue
    file_count = len(security_files) + len(other_files)
    response = input(str(file_count) + ' Files found to run against above database. Do you wish to continue? Y or N' +
                     os.linesep)
    if response != 'Y' and response != 'y':
        return

    # Connect to the database and run files
    connection = pyodbc.connect(connection_string, autocommit=True)
    cursor = connection.cursor()
    run_files(security_files, cursor)
    run_files(other_files, cursor)
    # Close the connection
    connection.close()


def get_security_files_from_path(path):
    # Get sql files from security directory
    name_list = glob.iglob(path + '/**/Security/*.sql', recursive=True)
    # Convert generator to list and return
    return [file for file in name_list if 'Manifest.sql' not in file
            and 'PostDeployment.sql' not in file]


def get_other_files_from_path(path):
    # Get all sql files from the path
    name_list = glob.iglob(path + '/**/*.sql', recursive=True)
    # Return file names that do not have Security in the path
    return [file for file in name_list if 'Security' not in file
            and 'Manifest.sql' not in file
            and 'PostDeployment.sql' not in file]


def run_files(file_names, cursor):
    try_later = []
    # Get scripts from files and run them
    for file_name in file_names:
        print(file_name)
        try:
            file_lines = get_lines_from_file(file_name, 'utf-8-sig')
        except UnicodeDecodeError:
            file_lines = get_lines_from_file(file_name, 'utf-16')
        failed_statements = run_lines(cursor, file_lines)
        # If there are any failed statements then add them to try-later list
        if failed_statements:
            try_later += failed_statements

    # If there are any statements that failed, try them again
    if len(try_later):
        for script in try_later:
            print('Retrying script:')
            failed_again, e = run_script(cursor, script)
            if failed_again:
                print(ScreenColors.FAIL + 'Retry Failed.' + ScreenColors.END_C)
                print(ScreenColors.FAIL + e.args[1] + ScreenColors.END_C)


def get_lines_from_file(file, file_encoding):
    # Open file for reading
    with open(file, 'r', encoding=file_encoding) as fp:
        # Get file contents into list and return it
        file_contents = fp.readlines()
        return list(file_contents)


def run_lines(cursor, file_lines):
    try_later = []
    script = ''
    for line in file_lines:
        line_compare = line.lower()
        if line_compare.startswith('go'):
            failed_script, e = run_script(cursor, script)
            if failed_script:
                print(ScreenColors.FAIL + 'Statement(s) failed. Saving for retry.' + ScreenColors.END_C)
                print(ScreenColors.FAIL + e.args[1] + ScreenColors.END_C)
                try_later.append(failed_script)
            script = ''
        else:
            script += line

    run_script(cursor, script)
    return try_later


def run_script(cursor, script):
    if script == '' or script.isspace():
        return None, None

    # Try running script and if it fails then return it
    try:
        if level == 'verbose':
            print(script)
        cursor.execute(script)
    except (pyodbc.IntegrityError, pyodbc.ProgrammingError, pyodbc.Error) as e:
        message = e.args[1]
        if 'already' in message:
            return None, None
        return script, e

    return None, None


def main(argv):
    print('Running main.')

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
    database_prefix = None
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
            database_prefix = arg
        if opt in ('-u', '--user'):
            user = arg
        if opt in ('-w', '--password'):
            password = arg

    if path is None or server is None or database_prefix is None or user is None or password is None:
        usage()
        sys.exit(4)

    sub_directories = [os.path.join(path, x) for x in os.listdir(path) if os.path.isdir(os.path.join(path, x))]
    for directory in sub_directories:
        if 'Application' in directory:
            get_and_run_files_from_path(database_prefix + 'Application', password, directory, server, user)
        elif 'DashboardDW' in directory:
            get_and_run_files_from_path(database_prefix + 'DashboardDW', password, directory, server, user)
        elif 'Dashboard' in directory:
            get_and_run_files_from_path(database_prefix + 'Dashboard', password, directory, server, user)


if __name__ == '__main__':
    main(sys.argv[1:])
