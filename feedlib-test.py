
import unittest
import feedlib

# --- nice_time

class NiceTimeTest(unittest.TestCase):

    def test_0(self):
        self.assertEqual("0 seconds", feedlib.nice_time(0))

    def test_2(self):
        self.assertEqual("2 seconds", feedlib.nice_time(2))

    def test_120(self):
        self.assertEqual("2 minutes", feedlib.nice_time(120))

    def test_3601(self):
        self.assertEqual("1 hours", feedlib.nice_time(3601))

    def test_7260(self):
        self.assertEqual("2 hours 1 mins", feedlib.nice_time(7260))

    def test_86401(self):
        self.assertEqual("1 days", feedlib.nice_time(86401))
        
# --- main

if __name__ == "__main__":
    unittest.main()
