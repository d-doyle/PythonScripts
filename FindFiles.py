import os
import re


def get_files_from_path(path):
    # Get list of files from path
    # name_list = os.listdir(path)
    # Join the path to the file names
    # full_list = [os.path.join(path, i) for i in name_list]
    full_list = [(dp, f) for dp, dn, fn in os.walk(path) for f in fn]
    # Return the file list
    return full_list


def main():
    print('Running main.')

    drive = 'J'
    folder = 'Mp3All'
    path = '%s:/%s/' % (drive, folder)
    pattern = r'^[\d]+[^\n]+.mp3'
    file_list = get_files_from_path(path)
    i = 0
    for directory, file in file_list:
        # print(file)
        match_object = re.search(pattern, file, flags=0)
        if match_object:
            i = i + 1
            print(os.path.join(directory, file))
    print(str(i) + ' files found')


if __name__ == '__main__':
    main()
