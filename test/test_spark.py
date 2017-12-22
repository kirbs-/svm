from nose.tools import *
import svm.spark as svm
import os.path
import os


class TestSpark(object):

    def test_spark_download(self):
        svm.Spark.download_source('2.2.0')
        filename = os.path.join(svm.Spark.HOME_DIR, svm.Spark.SVM_DIR, 'v2.2.0.zip')
        assert(os.path.isfile(filename))

    def test_spark_unzip(self):
        filename = os.path.join(svm.Spark.HOME_DIR, svm.Spark.SVM_DIR, 'v2.2.0.zip')
        assert(svm.Spark.unzip(filename))

    def test_spark_source_folder_rename(self):
        os.mkdir(os.path.join(svm.Spark.svm_path(), 'apache-spark-testfolder'))
        svm.Spark.rename_unzipped_folder('test2.2.0')
        version_folder = os.path.join(svm.Spark.svm_path(), 'vtest2.2.0')
        assert(os.path.exists(version_folder))
        os.rmdir(version_folder)

    @raises(svm.SparkInstallationError)
    def test_spark_source_folder_rename_exception_no_folder_found(self):
        svm.Spark.rename_unzipped_folder('test2.2.0')

    def test_spark_java_check(self):
        assert(svm.Spark.has_java())

    def test_spark_maven_check(self):
        assert(svm.Spark.has_maven())

    def test_spark_source_install(self):
        svm.Spark.install('2.2.0')

    def test_source_build(self):
        svm.Spark.build_from_source('2.2.0')
