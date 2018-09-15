import RunSqlFilesEdFi
import pyodbc
import unittest

path = 'D:\Projects\DelawareDOE\Dashboards-Plugin-EWS\Database'


class TestRunSqlFiles(unittest.TestCase):
    def test_get_other_files(self):
        files = RunSqlFilesEdFi.get_other_files_from_path(path)
        self.assertIsNotNone(files)
        self.assertGreater(len(files), 1)
        self.assertNotIn('Security', files[0])

    def test_get_security_files(self):
        files = RunSqlFilesEdFi.get_security_files_from_path(path)
        self.assertIsNotNone(files)
        self.assertGreater(len(files), 1)
        self.assertIn('Security', files[0])

    def test_get_lines_from_file(self):
        files = RunSqlFilesEdFi.get_other_files_from_path(path)
        self.assertIsNotNone(files)
        self.assertGreater(len(files), 1)
        lines = RunSqlFilesEdFi.get_lines_from_file(files[0], 'utf-8-sig')
        self.assertIsNotNone(lines)
        self.assertGreater(len(lines), 1)

    def test_get_security_files(self):
        cursor = pyodbc.Cursor()
        result = RunSqlFilesEdFi.run_script(cursor, 'select * from metric.Metric')
        self.assertIsNone(result)


suite = unittest.TestLoader().loadTestsFromTestCase(TestRunSqlFiles)
unittest.TextTestRunner(verbosity=2).run(suite)