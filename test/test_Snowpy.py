import unittest, sys, os

# parent directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import Snowpy


class TestSnowpy(unittest.TestCase):
    def test_connections(self):
        connections = Snowpy.connections()
        self.assertTrue('dev' in connections)


if __name__ == '__main__':
    unittest.main()
