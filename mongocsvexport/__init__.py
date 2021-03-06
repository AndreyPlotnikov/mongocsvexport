"""Export collection to csv file"""
import json
import sys
import argparse
import itertools
import pymongo
import csv
import types
import bson.json_util
from collections import OrderedDict
import tqdm
#from tqdm import tqdm



def get_params(prefix, args):
    """Extract params with given prefix from args dict"""
    params = {}
    for k,v in args.iteritems():
        if k.startswith(prefix) and v:
            params[k[len(prefix):]] = v
    return params

def flatten_iters_tree(l):
    for item in l:
        if type(item) in (types.GeneratorType, list, tuple):
            for subitem in flatten_iters_tree(item):
                yield subitem
        else:
            yield item

def tuple_startswith(t1, t2):
    t1_len = len(t1)
    for i, item in enumerate(t2):
        if i >= t1_len:
            return False
        if item != t1[i]:
            return False
    return True

class FieldValue(object):

    __slots__ = ('path', 'value')

    def __init__(self, path, value):
        self.path = path
        self.value = value

    def __repr__(self):
        return 'FieldValue({}, {})'.format(repr(self.path), repr(self.value))

def expand_dict(doc, fields, path=()):

    local_fields = [f[-1] for f in fields if tuple_startswith(f, path) and len(f) - len(path) == 1]
    absent_fields = []
    for local_field in local_fields:
        if local_field not in doc:
            absent_fields.append(local_field)

    def check_field(path, exact=True):
        compare = (lambda p1, p2: p1 == p2) if exact else (lambda p1, p2: tuple_startswith(p1, p2))
        for field in fields:
            if compare(field, path):
                return True
        return False

    def expand_list(l, fields, path=()):
        for item in l:
            if isinstance(item, dict):
                for subitem in expand_dict(item, fields, path):
                    yield subitem
            else:
                yield FieldValue(path, item)

    def tuple_iter():
        for gr_key, group in itertools.groupby(
                itertools.chain(doc.iteritems(), ((f, None) for f in absent_fields)),
                lambda val: 2 if type(val[1]) in (types.GeneratorType, list) else 1 if isinstance(val[1], dict) else 0):
            if gr_key == 0:
                def field_filter():
                    for k,v in group:
                        fpath = path + (k,)
                        if check_field(fpath, True):
                            yield FieldValue(fpath, v)
                yield [tuple(field_filter())]
            else:
                expand = expand_dict if gr_key == 1 else expand_list
                for k,v in group:
                    fpath = path + (k,)
                    if check_field(fpath, False):
                        yield expand(v, fields, fpath)

    tuples = tuple_iter()
    tuples = itertools.product(*tuples)
    tuples = (tuple(flatten_iters_tree(t)) for t in tuples)
    return tuples


class MongoExport(object):

    defaults = {
        'limit': None, # Limit number of documents to export
        'null_value': '',
        'delimiter': ',',
        'query_cond': None,
        'header': False,
        'psql_dump': None,
        'show_progress': False,
    }

    def __init__(self, collection, fields, output, config):
        self.config = self.defaults.copy()
        self.config.update(config)
        self.collection = collection
        self.fields = fields
        self._output = self._init_output(output)
        self._writer = csv.writer(self._output, delimiter=self.config['delimiter'])
        self._init_fields(fields)
        if self.config['psql_dump']:
            self.config['delimiter'] = ','
            self.config['header'] = False
            self.config['null_value'] = ''

    @classmethod
    def create(cls, db_name, coll_name, fields, output, config):
        """Constructor which initialize new mongo connection from config"""
        mongo_params = get_params('mongo_', config)
        conn = pymongo.MongoClient(**mongo_params)
        collection = conn[db_name][coll_name]
        return cls(collection, fields, output, config)

    def run(self):
        if self.config['header']:
            self._writer.writerow(self.fields)
        if self.config['psql_dump']:
            self._output.write('COPY {} FROM stdin WITH (FORMAT csv);\n'.format(
                self.config['psql_dump']))
        for doc in self._doc_iter():
            for row in self._get_rows(doc):
                self._writer.writerow(row)
        if self.config['psql_dump']:
            self._output.write('\\.\n')

    def _init_fields(self, fields):
        self._fields_order_map = OrderedDict()
        for i, field in enumerate(fields):
            path = tuple(field.split('.'))
            self._fields_order_map[path] = i

    def _init_output(self, output):
        if isinstance(output, basestring):
            output = open(output, 'w')
        return output

    def _doc_iter(self):
        limit = self.config['limit']
        query_cond = self.config['query_cond']
        args = (query_cond,) if query_cond else ()
        cursor = self.collection.find(*args)
        num_docs = cursor.count()
        if self.config['show_progress']:
            if limit:
                num_docs = limit
            cursor = tqdm.tqdm(cursor, total=num_docs)
        for i, doc in enumerate(cursor):
            if limit and i >= limit:
                break
            yield doc

    def _get_rows(self, doc):
        # generate output rows for given document
        for row in expand_dict(doc, self._fields_order_map.keys()):
            row = sorted(row, key=lambda fv: self._fields_order_map[fv.path])
            row = tuple(self._serialize(f.value) for f in row)
            yield row

    def _serialize(self, val):
        if val is None:
            val = self.config['null_value']
        elif isinstance(val, unicode):
            val = val.encode('utf-8')
        else:
            val = str(val)
        return val

def bson_object(val):
    """Create BSON-compatible dict from JSON-string"""
    return bson.json_util.loads(val)

def main():
    parser = argparse.ArgumentParser(argument_default=argparse.SUPPRESS)
    parser.add_argument('-d', dest='db_name', required=True,
                        help='Database name')
    parser.add_argument('-c', dest='coll_name', required=True,
                        help='Collection name')
    parser.add_argument('-f', dest='fields', required=True,
                        help='Comma separated list of fields')
    parser.add_argument('--host', dest='mongo_host',
                        help='Mongo connection')
    parser.add_argument('-o', dest='output_file', default=None,
                        help='Output file name. If not specified print to STDOUT')
    parser.add_argument('--limit', dest='limit', type=int,
                        help='Max number of documents')
    parser.add_argument('--null', dest='null_value',
                        help='NULL value replacement (default is empty string)')
    parser.add_argument('--cond', dest='query_cond', type=bson_object,
                        help='Mongo query condition in form of JSON-object')
    parser.add_argument('--delimiter', dest='delimiter',
                        help='Fields delimiter (default is comma)')
    parser.add_argument('--header', dest='header', action='store_true',
                        help='Output a header line with the name of each column')
    parser.add_argument('--psql-dump', dest='psql_dump',
                        help='Output data as psql dump file')
    parser.add_argument('-p', dest='show_progress', action='store_true',
                        help='Show progress meter')
    args = parser.parse_args()
    args = vars(args)
    if args.get('show_progress') and not args.get('output_file'):
        parser.error('You must use the -o option with -p')
    fields = args.pop('fields')
    fields = [f.strip() for f in fields.split(',')]
    coll_name = args.pop('coll_name')
    db_name = args.pop('db_name')
    output = args.pop('output_file')
    if output:
        output = open(output, 'w')
    else:
        output = sys.stdout
    export = MongoExport.create(db_name, coll_name, fields, output, args)
    try:
        export.run()
    except KeyboardInterrupt:
        print >>sys.stderr, "Keyboard interrupt. Exiting..."
        sys.exit(1)

if __name__ == '__main__':
    main()






