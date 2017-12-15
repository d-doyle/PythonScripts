import re

start_at = 11221809
increment_by = 11199652 - 11221809


def find_lines_in_file(file, file_encoding, pattern, process_match):
    # Open file for reading
    with open(file, 'r', encoding=file_encoding) as fp:
        # Get file contents into list
        file_contents = fp.readlines()
        # For each line in file
        for line in file_contents:
            # Search for the pattern
            match_object = re.search(pattern, line, flags=0)
            # Call function to process match
            process_match(match_object)


def show_match(match_object):
    if match_object:
        full_match = match_object.group(0)
        print(full_match)
        match_to_process = match_object.group(2)
        val = int(match_to_process)
        if val >= start_at:
            print(val + increment_by)


def replace_lines_in_file(file, file_encoding, pattern, process_match):
    # Open file for reading and writing
    with open(file, 'r+', encoding=file_encoding) as fp:
        # Get file contents into list
        file_contents = fp.readlines()
        # For each line in file
        for i in range(0, len(file_contents)) :
            line = file_contents[i]
            # Search for the pattern
            match_object = re.search(pattern, line, flags=0)
            # Call function to process match
            file_contents[i] = process_match(line, match_object)

        # Show updated file contents
        for line in file_contents:
            print(line)

        # Clear file
        fp.seek(0)
        fp.truncate()

        # Write data out to file
        fp.writelines(file_contents)


def update_match(line, match_object):
    if match_object:
        full_match = match_object.group(0)
        first_match_group = match_object.group(1)
        match_to_process = match_object.group(2)
        val = int(match_to_process)
        if val >= start_at:
            return line.replace(full_match, first_match_group + str(val + increment_by))

    return line


def main():
    print('Running main.')

    file_name = \
        'D:\Projects\Ed-Fi-Alliance\BulkLoad2_0\PopulatedTemplateBulkLoad\Data\StudentAssessment-Benchmarks-7th Grade ELA 2nd Six Weeks.xml'
    # MetadataList MetadataListId
    # pattern = r'(VALUES \()([\d]{2})'

    # MetadataListColumnGroup MetadataListId
    # pattern = r'(VALUES \([\d]{3}, )([\d]{2})'
    # MetadataListColumnGroup MetadataListColumnGroupId
    # pattern = r'(VALUES \()([\d]{3})'

    # MetadataListColumn MetadataListColumnGroupId
    # pattern = r'(VALUES \([\d]{4}, )([\d]{3})'
    # MetadataListColumn MetadataListColumnId
    # pattern = r'(VALUES \()([\d]{4})'

    # Student Assessment Item Identification Code
    pattern = r'([\s]+' + re.escape('<') + 'IdentificationCode' + re.escape('>') + ')([\d]{8})'
    show = False

    try:
        if show:
            find_lines_in_file(file_name, 'utf-8-sig', pattern, show_match)
        else:
            replace_lines_in_file(file_name, 'utf-8-sig', pattern, update_match)
    except UnicodeDecodeError:
        if show:
            find_lines_in_file(file_name, 'utf-16', pattern, show_match)
        else:
            replace_lines_in_file(file_name, 'utf-16', pattern, update_match)


if __name__ == '__main__':
    main()
