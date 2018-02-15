import asyncio
import sys
import getopt
import glob
import os
import subprocess


def usage():
    print('''\
    RunTests: Runs NUnit tests
    Usage: LoadCsvFiles [OPTIONS]
         -h --help                  Display this usage message
         -p --paths                 Required. Paths to compiled (DLL) Test files
         -s --search                Required. Search criteria for finding tests
         -a --all                   Run all tests at once instead of one at a time
         -t --test                  Only display the statements that would be run. Do not run them
    Example: RunTests -p D:\Projects\Ed-Fi-Alliance\Ed-Fi-ODS;D:\Projects\Ed-Fi-Alliance\Ed-Fi-ODS-Implementation \
-s '/**/bin/Debug/*.Tests.dll'
    Example: RunTests -p D:\Projects\Ed-Fi-Alliance\Ed-Fi-ODS -t
    ''')


def get_args(opts):
    paths = None
    search = None
    all_tests = False
    test = False
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage()
            sys.exit(3)
        if opt in ('-p', '--paths'):
            paths = arg.split(';')
        if opt in ('-s', '--search'):
            search = arg
        if opt in ('-a', '--all'):
            all_tests = True
        if opt in ('-t', '--test'):
            test = True
    return paths, search, all_tests, test


def get_files_from_path(path, search):
    search_path = path + search
    print('Using search path %s' % search_path)
    # Get list of files from path
    name_list = glob.iglob(search_path, recursive=True)
    # Join the path to the file names
    full_list = [os.path.join(path, i) for i in name_list]
    # Return the file list
    return full_list


def run_win_cmd(cmd):
    result = []
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in process.stdout:
        result.append(line)
    errcode = process.returncode
    for line in result:
        print(line)
    if errcode is not None:
        raise Exception('Command %s failed, see above for details', cmd)


def run_cmd_async(cmd):
    loop = asyncio.ProactorEventLoop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(run_command_shell(cmd))
    loop.close()
    return result


async def read_out(stream):
    while True:
        line = await stream.readline()
        if line:
            print(line.decode().strip('\r\n'))
        else:
            break


async def read_err(stream):
    while True:
        line = await stream.readline()
        if line:
            print('Error: %s' % line.decode().strip('\r\n'))
        else:
            break


async def run_command_shell(command):
    # Run command in subprocess (shell)

    # Create subprocess
    process = await asyncio.create_subprocess_shell(
        command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

    # Status
    print('Started:', command, '(pid = ' + str(process.pid) + ')')

    await asyncio.wait([
        read_out(process.stdout),
        read_err(process.stderr)
    ])

    return await process.wait()


def get_command(file):
    cmd = '"C:\\Program Files (x86)\\NUnit.org\\nunit-console\\nunit3-console.exe" ' + file
    return cmd


def get_all_command(files):
    cmd = '"C:\\Program Files (x86)\\NUnit.org\\nunit-console\\nunit3-console.exe" ' + ' '.join(files)
    return cmd


def main(argv):
    print('Running main.')

    # Get command line options
    try:
        opts, args = getopt.getopt(
            argv, 'hp:s:at', ['help', 'path=', 'search=', 'all', 'test'])
    except getopt.GetoptError:
        usage()
        sys.exit(1)

    # If options are empty exit
    if not len(opts):
        usage()
        sys.exit(2)

    # Get arguments
    paths, search, all_tests, test = get_args(opts)
    should_execute = not test
    if test:
        print('Performing test run only. Statements will not be executed against the database.')

    # If arguments are missing, show usage and exit
    if paths is None or search is None:
        usage()
        sys.exit(4)

    test_files = []
    # Get files from path
    for path in paths:
        print('Finding files in  %s' % path)
        files = get_files_from_path(path, search)
        print('Found %i files' % len(files))
        for file in files:
            print(file)
        test_files += files

    if not should_execute:
        sys.exit(0)

    if all_tests:
        cmd = get_all_command(test_files)
        print(cmd)
        result = run_cmd_async(cmd)
        print(result)
        if result != 0 and result != 4294967294:
            print('ERROR FAILING TEST FOUND!')
        else:
            print('All tests passed!')
    else:
        has_error = False
        for file in test_files:
            cmd = get_command(file)
            print(cmd)
            result = run_cmd_async(cmd)
            print(result)
            if result != 0 and result != 4294967294:
                has_error = True
                print('ERROR FAILING TEST FOUND!')
                break

        if not has_error:
            print ('All tests passed!')


if __name__ == '__main__':
    main(sys.argv[1:])
