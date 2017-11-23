import getopt
import os
import pyodbc
import sys


def usage():
    print("""\
    CreateInsert: Create inserts for the metric.MetadataListColumn table, \
    and UTF-8-BOM encoded files
    Usage: CreateInsert [OPTIONS]
         -h --help                  Display this usage message
         -p --path                  Required. Path for creating Insert SQL files
         -s --server                Required. Database server
         -d --database              Required. Database name
         -u --user                  Required. User name
         -w --password              Required. Password
    Example: CreateInsert -p D:\Projects\Indiana\Dashboards-Plugin-EWS\Database\Data\Dashboard\DashboardTypes -s . \
-d IN_EdFi_Dashboard -u edfiPService -w edfiPService
    Example: CreateInsert \
-p D:\Projects\Indiana\Ed-Fi-Dashboard\Etl\src\EdFi.Runtime\Reading\Queries\2.0\DashboardTypes \
-s . -d IN_EdFi_Dashboard -u edfiPService -w edfiPService
    """)


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

    # Build the connection string
    connection_string = 'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + \
                        user + ';PWD=' + password
    print(connection_string)

    # Setup statements
    group_statement = \
        "select MetadataListColumnGroupId from metric.MetadataListColumnGroup where Title = 'ASSESSMENTS'"

    metric_statement = """ 
        select MetricId, MetricName from metric.Metric where MetricId in (
            select MetricId from metric.MetricNode where ParentNodeId in (
                select MetricNodeId from metric.MetricNode where ParentNodeId in (
                    select MetricNodeId from metric.MetricNode where ParentNodeId in (
                        select MetricNodeId from metric.MetricNode where MetricId in (
                            select MetricId from metric.Metric where MetricName = 'State Assessments'
                        )
                    )
                )
            )
        ) and MetricId not in (select distinct MetricVariantId from metric.MetadataListColumn)
         and DomainEntityTypeId = 1
    """

    # Connect to the database
    connection = pyodbc.connect(connection_string, autocommit=False)
    cursor = connection.cursor()

    # Get group result
    group_result = cursor.execute(group_statement).fetchall()
    metadata_list_column_group_id_list = [x[0] for x in group_result]
    print(metadata_list_column_group_id_list)

    # Get metric result
    metric_result = cursor.execute(metric_statement).fetchall()
    print(metric_result)

    metadata_list_column_id = 1000
    # Open the file
    with open(os.path.join(path, 'InsertNewMetadataColumns.sql'), 'a', encoding='utf-8') as fp:
        # Clear the file
        fp.seek(0)
        fp.truncate()
        # For each group
        for group_id in metadata_list_column_group_id_list:
            column_order = 7
            # For each metric
            for metric in metric_result:
                # Create an insert statement
                metric_id, metric_name = metric
                is_visible_by_default = 1
                is_fixed_column = 0
                metric_cell_type = 3
                insert_stmt = f"""
INSERT INTO [metric].[MetadataListColumn]
([MetadataListColumnId], [MetadataListColumnGroupId], [ColumnName], [ColumnPrefix], [IsVisibleByDefault], [IsFixedColumn], [MetadataMetricCellListTypeId], [MetricVariantId], [ColumnOrder], [SortAscending], [SortDescending], [Tooltip], [UniqueIdentifier])
VALUES
({metadata_list_column_id}, {group_id}, '{metric_name}', '{metric_name}', {is_visible_by_default}, {is_fixed_column}, {metric_cell_type}, {metric_id}, {column_order}, NULL, NULL, '', {metric_id})
"""
                # Show it and write it to a file
                print(insert_stmt)
                fp.write(insert_stmt)
                # Increment the id and column order
                metadata_list_column_id += 1
                column_order += 1

    # Close the connection
    connection.commit()
    connection.close()


if __name__ == '__main__':
    main(sys.argv[1:])

