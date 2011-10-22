"""
A test suite for various parts of whazzup.
"""

import dbqueue, dbimpl

import unittest

# ===========================================================================
# QUEUE TEST

class QueueTest(unittest.TestCase):

    def test_very_basic(self):
        dbimpl.mqueue.send("foo bar")
        self.assertEqual(dbqueue.recv_mqueue.get_next_message(), "foo bar")

    def test_filling_up_queue(self):
        for ix in range(50):
            dbimpl.mqueue.send("foo %s" % ix)

        for ix in range(50):
            msg = dbqueue.recv_mqueue.get_next_message()
            if not msg:
                # this means that some messages are stuck in the sending queue.
                # need to send another message to flush them out.
                dbimpl.mqueue.send("flush")
                msg = dbqueue.recv_mqueue.get_next_message()
            
            self.assertEqual(msg, "foo %s" % ix)

        msg = dbqueue.recv_mqueue.get_next_message()
        while msg:
            self.assertEqual(msg, "flush")
            msg = dbqueue.recv_mqueue.get_next_message()

    def test_priority(self):
        dbimpl.mqueue.send("low priority")
        dbimpl.mqueue.send("high priority", 2)
        self.assertEqual(dbqueue.recv_mqueue.get_next_message(),
                         "high priority")
        self.assertEqual(dbqueue.recv_mqueue.get_next_message(),
                         "low priority")
            
# ===========================================================================
# MAIN

if __name__ == '__main__':
    unittest.main()
