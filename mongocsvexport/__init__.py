"""Export collection to csv file"""
import sys
import argparse
import itertools
import pymongo
import csv

def get_params(prefix, args):
    """Extract params with given prefix from args dict"""
    params = {}
    for k,v in args.iteritems():
        if k.startswith(prefix) and v:
            params[k[len(prefix):]] = v
    return params


class MongoExport(object):

    defaults = {
        'limit': None, # Limit number of documents to export
        'null_value': '',
    }

    def __init__(self, collection, fields, output, config):
        self.fields = [f.split('.') for f in fields]
        self.config = self.defaults.copy()
        self.config.update(config)
        self.collection = collection
        self._output = self._init_output(output)
        self._writer = csv.writer(self._output)

    @classmethod
    def create(cls, db_name, coll_name, fields, output, config):
        """Constructor which initialize new mongo connection from config"""
        mongo_params = get_params('mongo_', config)
        conn = pymongo.MongoClient(**mongo_params)
        collection = conn[db_name][coll_name]
        return cls(collection, fields, output, config)

    def run(self):
        for doc in self._doc_iter():
            for row in self._get_rows(doc):
                self._writer.writerow(row)

    def _init_output(self, output):
        if isinstance(output, basestring):
            output = open(output, 'w')
        return output

    def _doc_iter(self):
        limit = self.config['limit']
        for i, doc in enumerate(self.collection.find()):
            if limit and i >= limit:
                break
            yield doc

    def _get_rows(self, doc):
        # generate output rows for given document
        rows = (self._get_values(doc, field) for field in self.fields)
        return itertools.product(*rows)

    def _get_values(self, doc, path):
        # generate all values for document's field
        field = path[0]
        if not field:
            val = doc
        else:
            val = doc.get(field)
        if not isinstance(val, list):
            val = [val]
        if len(path) > 1:
            for v in val:
                for subv in self._get_values(v, path[1:]):
                    yield subv
        else:
            for v in val:
                yield self._serialize(v)

    def _serialize(self, val):
        if val is None:
            val = self.config['null_value']
        elif isinstance(val, unicode):
            val = val.encode('utf-8')
        else:
            val = str(val)
        return val


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
    args = parser.parse_args()
    args = vars(args)
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
    export.run()

if __name__ == '__main__':
    main()






