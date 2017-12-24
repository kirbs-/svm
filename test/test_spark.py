from nose.tools import *
import svm.spark as svm
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

    def test_spark_source_install(self):
        svm.Spark.install('2.2.0')

    def test_source_build(self):
        svm.Spark.build_from_source('2.2.0')

    def test_activate_spark(self):
        svm.Spark.activate_spark('2.2.0')
        pyspark_file = os.path.join(os.sep,'usr','local','bin','pyspark')
        assert(os.path.exists(pyspark_file))

    def test_deactivate_spark(self):
        svm.Spark.deactivate_spark('2.2.0')


