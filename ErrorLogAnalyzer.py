import getopt
import os
import sys


class ErrorData(object):
    def __init__(self):
        self.Message = ''
        self.Source = ''
        self.Form = ''
        self.Url = ''
        self.UserName = ''
        self.QueryString = ''
        self.TargetSite = ''
        self.Exception = ''


def usage():
    print("""\
    Usage: ErrorLogAnalyzer [OPTIONS]
         -h --help                  Display this usage message
         -r --recent n              Display the n most recent errors
    """)


def main(argv):
    print("Running main.")
    try:
        opts, args = getopt.getopt(argv, 'f:r:h', ['file=', 'recent=', 'help'])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    recent = None
    file_name = None
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage()
            sys.exit(0)
        if opt in ('-r', '--recent'):
            recent = arg
        if opt in ('-f', '--file'):
            file_name = arg

    directory = 'C:\\Temp\\'

    if recent is not None:
        print('Showing recent.')
        result = most_recent(directory, int(recent))
        print(result)
        return

    print('Showing common.')
    most_common(directory, file_name)


def most_recent(directory, n):
    name_list = os.listdir(directory)
    full_list = [os.path.join(directory, i) for i in name_list]
    sorted_list = sorted(full_list, key=os.path.getmtime, reverse=True)

    file_errors = []
    all_errors = []
    error_lines = ''
    error_count = 0
    # For each file in the directory
    for f in sorted_list:
        # If it is an error file
        if 'Error' in f:
            # Open the file for reading
            error_file = open(f, 'r')
            # For each line in the file
            for line in error_file:
                # If we found the first line of the next error block
                if '@Project=' in line and len(error_lines) > 0:
                    file_errors.append(error_lines)
                    error_lines = ''
                    error_count += 1

                error_lines += line

            # If it's not an empty file
            if len(error_lines) > 0:
                # record last error in file
                file_errors.append(error_lines)
                error_lines = ''
                error_count += 1

            # revers file errors into all errors list because newest error was at bottom of the file
            all_errors.extend(file_errors[::-1])
            file_errors = []

            # If we have more than requested errors, get first n and return them
            if error_count > n:
                errors = '\n'
                for i in xrange(0, n):
                    errors += "Error: %s" % (i + 1) + '\n'
                    errors += all_errors[i] + '\n'
                return errors


def most_common(directory, file_name=None):
    message_dict = {}
    error_dict = {}
    error_objects = []
    if file_name is None:
        file_name = 'Error'

    for f in os.listdir(directory):
        if file_name in f:
            print('Analyzing ' + f)
            error_file = open(directory + f, 'r')
            section = ''
            error_data = ErrorData()
            for line in error_file:
                section = get_section(line, section)
                if section == 'Message':
                    if line not in message_dict:
                        message_dict[line] = 1
                    else:
                        message_dict[line] += 1

                if section != '':
                    setattr(error_data, section, getattr(error_data, section) + ' ' + line)

                if '***ServerVariables:***' in line:
                    section = ''
                    error_objects.append(error_data)
                    if error_data.Message not in error_dict:
                        error_dict[error_data.Message] = [error_data]
                    else:
                        error_dict[error_data.Message].append(error_data)
                    error_data = ErrorData()

    for w in sorted(message_dict, key=message_dict.get):
        print(w, message_dict[w])


def get_section(line, section):
    if 'MESSAGE: ' in line:
        section = 'Message'
    elif 'SOURCE: ' in line:
        section = 'Source'
    elif 'FORM: ' in line:
        section = 'Form'
    elif 'URL: ' in line:
        section = 'Url'
    elif 'UserName: ' in line:
        section = 'UserName'
    elif 'QUERYSTRING: ' in line:
        section = 'QueryString'
    elif 'TARGETSITE: ' in line:
        section = 'TargetSite'
    elif 'EXCEPTION: ' in line:
        section = 'Exception'

    return section


if __name__ == '__main__':
    main(sys.argv[1:])
