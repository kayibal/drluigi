import time

import luigi
import os
import tempfile
from random import shuffle
import unittest

luigi.notifications.DEBUG = True

class ReaderTask(luigi.Task):
    file = luigi.Parameter()
    dummy_param = luigi.IntParameter()
    use_lock = luigi.BoolParameter()

    def run(self):
        if self.use_lock:
            self.rw_lock.acquire_read()
        with open(self.file, "r") as f:
            s = f.read()
        # print("WORKER {} : {}".format(os.getpid(),s))
        if self.use_lock:
            self.rw_lock.release()

class WriterTask(luigi.Task):
    file = luigi.Parameter()
    dummy_param = luigi.IntParameter()
    use_lock = luigi.BoolParameter()

    def run(self):
        if self.use_lock:
            self.rw_lock.acquire_write()
        for _ in range(1000):
            with open(self.file, "a") as f:
                f.write("This is a single line written by {}".format(os.getpid()))
                f.write("\n")
        if self.use_lock:
            self.rw_lock.release()

class RWLockTest(unittest.TestCase):
    def setUp(self):
        self.file = "/home/kayibal/tmp/luigi_rwlock.txt"#tempfile.mktemp()
        open(self.file,"w").close()

    # def testTaskNoLock(self):
    #     self.readers = [ReaderTask(file=self.file, use_lock=False, dummy_param=i) for i in range(50)]
    #     self.writers = [WriterTask(file=self.file, use_lock=False, dummy_param=i) for i in range(50)]
    #     tasks = self.readers + self.writers
    #     shuffle(tasks)
    #     luigi.build(tasks, local_scheduler=True, workers=30)
    #     with open(self.file, "r") as f:
    #         s = f.read()

    def testTaskLock(self):
        self.readers = [ReaderTask(file=self.file, use_lock=True, dummy_param=i) for i in range(50)]
        self.writers = [WriterTask(file=self.file, use_lock=True, dummy_param=i) for i in range(50)]
        tasks = self.readers + self.writers
        shuffle(tasks)
        luigi.build(tasks, local_scheduler=True, workers=30)
        with open(self.file, "r") as f:
            s = f.read().split("\n")
        last_s = s[0]
        s = s[1:]
        counter = 0
        counts = []
        for string in s:
            if string == last_s:
                counter += 1
            else:
                counts.append(counter)
                counter = 0
            last_s = string
        assert len(set(counts)) == 1

    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()