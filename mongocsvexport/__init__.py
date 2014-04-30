"""Export collection to PostgreSQL compliant csv-file"""
import sys
import argparse
import pymongo
import csv

def get_params(prefix, args):
    params = {}
    for k,v in args.iteritems():
        if k.startswith(prefix) and v:
            params[k[len(prefix):]] = v
    return params


class MongoExport(object):

    defaults = {
        'output_file': None,  # STDOUT by default
        'limit': None, # Limit number of document to export
    }

    def __init__(self, db_name, coll_name, fields, config):
        self.db_name = db_name
        self.coll_name = coll_name
        self.fields = fields
        self.config = self.defaults.copy()
        self.config.update(config)
        self._mongo = None
        self._output = None
        self._writer = None

    def export(self):
        conn = self._mongo_connect()
        coll = conn[self.db_name][self.coll_name]
        limit = self.config['limit']
        self._init_writer()
        for i, doc in enumerate(coll.find()):
            if limit and i >= limit:
                break
            row = self._get_row(doc)
            self._writer.writerow(row)

    def _mongo_connect(self):
        if self._mongo is not None:
            return self._mongo
        mongo_params = get_params('mongo_', self.config)
        self._mongo = pymongo.MongoClient(**mongo_params)
        return self._mongo

    def _init_writer(self):
        if self.config['output_file']:
            self._output = open(self.config['output_file'], 'w')
        else:
            self._output = sys.stdout
        self._writer = csv.writer(self._output)

    def _get_row(self, doc):
        row = []
        for field in self.fields:
            val = doc.get(field)
            if val is None:
                val = '\N'
            elif isinstance(val, unicode):
                val = val.encode('utf-8')
            else:
                val = str(val)
            row.append(val)
        return row

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', dest='db_name', required=True,
                        help='Database name')
    parser.add_argument('-c', dest='coll_name', required=True,
                        help='Collection name')
    parser.add_argument('-f', dest='fields', required=True,
                        help='Comma separated list of fields')
    parser.add_argument('--host', dest='mongo_host', default='localhost',
                        help='Mongo connection')
    parser.add_argument('-o', dest='output_file',
                        help='Output file name. If not specified print to STDOUT')
    parser.add_argument('--limit', dest='limit', type=int,
                        help='Max number of documents')
    args = parser.parse_args()
    args = vars(args)
    fields = args.pop('fields')
    fields = [f.strip() for f in fields.split(',')]
    coll_name = args.pop('coll_name')
    db_name = args.pop('db_name')
    export = MongoExport(db_name, coll_name, fields, args)
    export.export()

if __name__ == '__main__':
    main()






