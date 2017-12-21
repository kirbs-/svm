import requests
from os import path


class Spark(object):

    _spark_versions = {}
    SPARK_REPO_URL = 'https://api.github.com/repos/apache/spark'
    HOME_DIR = path.expanduser('~')
    SVM_DIR = '.svm'

    @classmethod
    @property
    def spark_versions(cls):
        if not cls._spark_versions:
            cls._spark_versions = {v['name']: v['zipball_url'] for v in requests.get(Spark.SPARK_REPO_URL + '/tags').json()}
        return cls._spark_versions

    @staticmethod
    def print_version_list():
        for version in sorted(Spark.spark_versions):
            print("[ ] {}".format(version[1:]))

    @staticmethod
    def download(version):
        """
        Download Spark version. Uses same name as release tag without the leading 'v'.
        :param version: Version number to download.
        :return: None
        """
        local_filename = path.join(Spark.HOME_DIR, Spark.SVM_DIR, 'v{}.zip'.format(version))
        r = requests.get(Spark.spark_versions['v{}'.format(version)], stream=True)
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
