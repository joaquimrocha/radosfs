from __future__ import print_function
import argparse
import errno
import radosfs
import unittest
import sys

confPath = ''
confUser = ''
dataPool = ''
metadataPool = ''

class RadosFsTestBase(unittest.TestCase):

    fs = radosfs.Filesystem()

    def setUp(self):
        self.assertEqual(self.fs.init(confUser, confPath), 0)

    def tearDown(self):
        pass

    def addPools(self):
        self.assertEqual(self.fs.addMetadataPool(metadataPool, '/'), 0)
        self.assertEqual(self.fs.addDataPool(dataPool, '/'), 0)

    def testPoolsAdditionAndRemoval(self):
        self.addPools()
        self.assertEqual(self.fs.removeMetadataPool(metadataPool), 0)
        self.assertEqual(self.fs.removeDataPool(dataPool), 0)

def setupArguments():
    parser = argparse.ArgumentParser()
    # change the name of the 'optional arguments' label, although this should
    # be accomplished in a better way than setting a private var
    parser._optionals.title = 'arguments'
    parser.add_argument('-c', '--conf', help="path to the Ceph cluster's "
                        "configuration file", required=True)
    parser.add_argument('-p', '--pools', help="the names of the pools to use "
                        "for the test", nargs=2,
                        metavar=("METADATA_POOL", "DATA_POOL"), required=True)
    parser.add_argument('-u', '--user', help="the user name to connect to the "
                        "cluster")
    return parser

def help():
    print(sys.argv[0] + ' --conf=')

if __name__ == '__main__':
    parser = setupArguments()
    args, unknown = parser.parse_known_args()
    sys.argv = [sys.argv[0]] + unknown

    confPath = args.conf
    confUser = args.user if args.user else ''
    metadataPool = args.pools[0]
    dataPool = args.pools[1]

    print('Running tests on "%s" with user "%s" with the metadata pool "%s" and '
          'data pool "%s"\n' % (confPath, confUser, metadataPool, dataPool))

    unittest.main()
