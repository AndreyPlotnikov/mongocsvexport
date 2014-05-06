# -*- coding: utf-8 -*-
import unittest
from datetime import datetime
import sys
from mock import patch, MagicMock
from StringIO import StringIO
from mongocsvexport import MongoExport, main


class CoreTests(unittest.TestCase):

    def setUp(self):
        self.output = StringIO()

    def create_instance(self, fields, docs, config={}):
        class MongoExportPatch(MongoExport):
            def _doc_iter(self):
                return iter(docs)
        export = MongoExportPatch(MagicMock(), fields, self.output, config)
        return export

    def test_defaults(self):
        export = self.create_instance(['f1','f2'],
                                      [{'f1':'foo1', 'f2': 'one,two'},{'f2':'foo2'}], {})
        export.run()
        result = self.output.getvalue()
        self.assertEqual(result,
                         'foo1,"one,two"\r\n,foo2\r\n')

    def test_unicode(self):
        export = self.create_instance(['f1','f2'],
                                      [{'f1':'foo1', 'f2': u'сепулька'}])
        export.run()
        self.assertEqual(self.output.getvalue(),
                         'foo1,\xd1\x81\xd0\xb5\xd0\xbf\xd1\x83\xd0\xbb\xd1\x8c\xd0\xba\xd0\xb0\r\n')

    def test_datetime(self):
        export = self.create_instance(['f1','f2'],
                                      [{'f1':datetime(2014,5,1,12,34,10),
                                        'f2': datetime(2014,5,1,12,0,10,345000)}])
        export.run()
        self.assertEqual(self.output.getvalue(),
                         '2014-05-01 12:34:10,2014-05-01 12:00:10.345000\r\n')

    def test_field_traverse(self):
        export = self.create_instance(['f1','f2.sub'],
                                      [{'f1':'foo1', 'f2': {'sub': 'foo2'}}], {})
        export.run()
        self.assertEqual(self.output.getvalue(),
                         'foo1,foo2\r\n')

    def test_trivial_list_expansion(self):
        export = self.create_instance(['f1','f2'],
                                      [{'f1':'foo1', 'f2': ['one','two']}], {})
        export.run()
        self.assertEqual(self.output.getvalue(),
                         'foo1,one\r\nfoo1,two\r\n')

    def test_doc_list_expansion(self):
        export = self.create_instance(['f1','f2.sub'],
                                      [{'f1':'foo1', 'f2': [{'sub':'one'},{'sub':'two'}]}], {})
        export.run()
        self.assertEqual(self.output.getvalue(),
                         'foo1,one\r\nfoo1,two\r\n')

    def test_doc_list_expansion_absent_field(self):
        export = self.create_instance(['f1','f2.sub'],
                                      [{'f1':'foo1', 'f2': [{'sub':'one'},{'sub2':'two'}]}], {})
        export.run()
        self.assertEqual(self.output.getvalue(),
                         'foo1,one\r\nfoo1,\r\n')


    def test_multiple_inner_lists_expansion(self):
        export = self.create_instance(['f1','f2.sub.sub2'],
                                      [{'f1':'foo1',
                                        'f2': [
                                            {'sub':[{'sub2': 'one'}, {'sub2': 'two'}]},
                                            {'sub':[{'sub2': 'three'}]}]}], {})
        export.run()
        print self.output.getvalue()
        # self.assertEqual(self.output.getvalue(),
        #                  'foo1,one\r\nfoo1,two\r\nfoo1,three\r\n')


    def test_several_sub_fields_lists_expansion(self):
        export = self.create_instance(['hotel', 'rooms.name', 'hotel_id', 'rooms.price'],
                                      [{'hotel':'Hilton',
                                        'rooms': [
                                            {'name': 'Standard', 'price': 100},
                                            {'name': 'Deluxe', 'price': 120}],
                                        'hotel_id': 1000
                                       }])
        export.run()
        # export = MongoExport(MagicMock(), [['hotel'],['rooms', 'name', 'price'], ['hotel_id']], MagicMock(), {})
        # result = export._get_rows(
        #     {'hotel':'Hilton',
        #      'rooms': [
        #          {'name': 'Standard', 'price': 100},
        #          {'name': 'Deluxe', 'price': 120}],
        #      'hotel_id': 1000
        #     })
        # #list(result)
        print self.output.getvalue()
        self.assertEqual(self.output.getvalue(),
                         'Hilton,Standard,1000,100\r\nHilton,Deluxe,1000,120\r\n')


class CreateTest(unittest.TestCase):
    """MongoExport.create constructor test"""

    def test_create(self):
        output = StringIO()
        config = {'limit': 100,
                  'mongo_host': 'localhost:27034'}
        fields = ['f1']
        with patch('pymongo.MongoClient') as mongo_mock:
            export = MongoExport.create('testdb', 'testcoll', fields,
                                        output, config)
        self.assertEqual(len(mongo_mock.call_args_list), 1)
        self.assertEqual(mongo_mock.call_args_list[0][1]['host'], 'localhost:27034')
        self.assertEqual(export.fields, ['f1'])
        self.assertEqual(export.config['limit'], 100)


class MongoInteropTests(unittest.TestCase):

    def setUp(self):
        self.collection = MagicMock()

    def create_instance(self, docs, config={}):
        def find_mock(*args, **kwargs):
            return docs
        self.collection.find.side_effect = find_mock
        export = MongoExport(self.collection, ['f1'], MagicMock(), config)
        return export

    def test_no_limit(self):
        export = self.create_instance([{'f1':'foo1'},{'f1':'foo2'}])
        result = list(export._doc_iter())
        self.assertEqual(len(result),2)
        self.assertEqual(result, [{'f1':'foo1'},{'f1':'foo2'}])

    def test_limit(self):
        export = self.create_instance([{'f1':'foo1'},{'f1':'foo2'}],
                                      {'limit': 1})
        result = list(export._doc_iter())
        self.assertEqual(len(result),1)
        self.assertEqual(result, [{'f1':'foo1'}])


class CmdRunTests(unittest.TestCase):

    @patch('sys.argv', ['mongocsvexport'])
    def test_empty_run(self):
        stderr = StringIO()
        with patch('sys.stderr', stderr):
            with self.assertRaises(SystemExit):
                main()
        output = stderr.getvalue()
        self.assertTrue('usage:' in output)
        self.assertTrue('argument -d is required' in output)

    @patch('sys.argv', ['mongocsvexport', '-d', 'testdb'])
    def test_coll_absent(self):
        stderr = StringIO()
        with patch('sys.stderr', stderr):
            with self.assertRaises(SystemExit):
                main()
        output = stderr.getvalue()
        self.assertTrue('usage:' in output)
        self.assertTrue('argument -c is required' in output)

    @patch('sys.argv', ['mongocsvexport', '-d', 'testdb', '-c', 'testcoll'])
    def test_fields_absent(self):
        stderr = StringIO()
        with patch('sys.stderr', stderr):
            with self.assertRaises(SystemExit):
                main()
        output = stderr.getvalue()
        self.assertTrue('usage:' in output)
        self.assertTrue('argument -f is required' in output)

    def _run_main(self):
        with patch('mongocsvexport.MongoExport.create') as create_mock:
            with patch('mongocsvexport.MongoExport.run'):
                main()
        return create_mock.call_args[0]

    required_args = ['mongocsvexport', '-d', 'testdb', '-c', 'testcoll', '-f', 'f1.sub,f2']

    @patch('sys.argv',
           ['mongocsvexport', '-d', 'testdb', '-c', 'testcoll', '-f', 'f1.sub,f2'])
    def test_required_args(self):
        args = self._run_main()
        self.assertEqual(args[0], 'testdb')
        self.assertEqual(args[1], 'testcoll')
        self.assertEqual(args[2], ['f1.sub','f2'])
        self.assertEqual(args[3], sys.stdout)
        self.assertEqual(args[4], {})

    @patch('sys.argv', required_args + ['--limit', '100'])
    def test_limit(self):
        args = self._run_main()
        self.assertEqual(args[4], {'limit': 100})

    @patch('sys.argv', required_args + ['-o', '/tmp/data.csv'])
    def test_output_file(self):
        with patch('__builtin__.open') as open_mock:
            args = self._run_main()
        self.assertEqual(open_mock.call_args[0][0], '/tmp/data.csv')

    @patch('sys.argv', required_args + ['--host', 'localhost:27034'])
    def test_host(self):
        args = self._run_main()
        self.assertEqual(args[4], {'mongo_host': 'localhost:27034'})
