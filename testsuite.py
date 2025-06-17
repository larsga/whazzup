"""
A test suite for various parts of whazzup.
"""

import dbqueue, dbimpl, feedlib

import unittest

# ===========================================================================
# QUEUE

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
# DATE PARSING

class DateParsingTest(unittest.TestCase):

    # FIXME: this one doesn't work
    def _test_format_1(self):
        str = "Sun Jan 16 15:55:53 UTC 2011"
        date = feedlib.parse_date(str)
        print date

    def test_format_2(self):
        date = feedlib.parse_date("Sun, 16 January 2011 07:13:33")
        self.assertEqual(str(date), '2011-01-16 07:13:33')

    # FIXME: gets the seconds wrong, incredibly
    def _test_format_3(self):
        date = feedlib.parse_date('Sat, 22 Oct 2011 08:22:53 +0000')
        self.assertEqual(str(date), '2011-10-22 08:22:53')

    def test_format_4(self):
        date = feedlib.parse_date('2011-10-13T22:09:45.314+02:00')
        self.assertEqual(str(date), '2011-10-13 22:09:45')

    def test_format_5(self):
        date = feedlib.parse_date('2011-10-21T17:30:00Z')
        self.assertEqual(str(date), '2011-10-21 17:30:00')

# ===========================================================================
# FEED DATABASE

class FeedDatabaseTest(unittest.TestCase):

    def setUp(self):
        # empty the database
        dbimpl.dbconn.update('delete from feeds', None)
        dbimpl.dbconn.commit()

    def test_user_count(self):
        self.assertEqual(0, dbimpl.feeddb.get_user_count())

    def test_feed_count(self):
        self.assertEqual(0, dbimpl.feeddb.get_feed_count())

    def test_post_count(self):
        self.assertEqual(0, dbimpl.feeddb.get_post_count())

    def test_rated_posts_count(self):
        self.assertEqual(0, dbimpl.feeddb.get_rated_posts_count())

    def test_read_posts_count(self):
        self.assertEqual(0, dbimpl.feeddb.get_read_posts_count())

    def test_notification_count(self):
        self.assertEqual(0, dbimpl.feeddb.get_notification_count())

    def test_add_feed(self):
        feeddb = dbimpl.feeddb

        feed = feeddb.add_feed('http://test.no')
        dbimpl.dbconn.commit()
        self.assertEqual(1, feeddb.get_feed_count())

        feed2 = feeddb.get_feed_by_id(feed.get_local_id())
        self.assertEqual(feed.get_url(), feed2.get_url())
        self.assertEqual(0, feed2.get_item_count())
        feed.delete()
        dbimpl.dbconn.commit()

        self.assertEqual(0, feeddb.get_feed_count())

        feed2 = feeddb.get_feed_by_id(feed.get_local_id())
        self.assertEqual(None, feed2)

# ===========================================================================
# MAIN

if __name__ == '__main__':
    unittest.main()
