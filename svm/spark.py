import requests
import os
import zipfile
import glob
import fnmatch


class Spark(object):

    _spark_versions = {}
    SPARK_REPO_URL = 'https://api.github.com/repos/apache/spark'
    HOME_DIR = os.path.expanduser('~')
    SVM_DIR = '.svm'

    @classmethod
    def spark_versions(cls):
        if not cls._spark_versions:
            cls._spark_versions = {v['name']: v['zipball_url'] for v in requests.get(Spark.SPARK_REPO_URL + '/tags').json()}
        return cls._spark_versions

    @staticmethod
    def print_version_list():
        for version in sorted(Spark.spark_versions()):
            print("[ ] {}".format(version[1:]))

    @staticmethod
    def download(version):
        """
        Download Spark version. Uses same name as release tag without the leading 'v'.
        :param version: Version number to download.
        :return: None
        """
        local_filename = 'v{}.zip'.format(Spark.svm_version_path(version))
        r = requests.get(Spark.spark_versions()['v{}'.format(version)], stream=True)
        os.makedirs(os.path.dirname(local_filename), exist_ok=True)
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)

    @staticmethod
    def svm_version_path(version):
        """
        Path to specified spark version. Accepts semantic version numbering.
        :param version: Spark version as String
        :return: String.
        """
        return os.path.join(Spark.HOME_DIR, Spark.SVM_DIR, 'v{}'.format(version))

    @staticmethod
    def svm_path():
        """
        Path to local Spark verions folder. Defaults to user_home/.svm
        :return: String
        """
        return os.path.join(Spark.HOME_DIR, Spark.SVM_DIR )

    @staticmethod
    def unzip(filename):
        with zipfile.ZipFile(filename, "r") as zip_ref:
            zip_ref.extractall(Spark.svm_path())
        return True

    @staticmethod
    def rename_unzipped_folder(version):
        """
        Renames unzipped spark version folder to the release tag.
        :param version: version from release tag.
        :return:
        """

        for filename in os.listdir(Spark.svm_path()):
            if fnmatch.fnmatch(filename, 'apache-spark-*'):
                return os.rename(os.path.join(Spark.svm_path(), filename), Spark.svm_version_path(version))

        raise SparkError("Unable to find unzipped Spark folder in {}". format(Spark.svm_path()))


class SparkError(AssertionError):

    def __init__(self, *args, **kwargs):
        AssertionError.__init__(self, *args, **kwargs)