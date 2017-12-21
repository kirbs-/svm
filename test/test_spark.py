from nose.tools import *
import svm.spark as svm
import os.path


class TestSpark(object):

    def test_spark_download(self):
        svm.Spark.download('2.2.0')
        filename = os.path.join(svm.Spark.HOME_DIR, svm.Spark.SVM_DIR, 'v2.2.0.zip')
        assert(os.path.isfile(filename))