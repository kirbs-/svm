from setuptools import setup

setup(name='svm',
      version='0.0.1',
      url='https://github.com/kirbs/svm',
      description='Version manager for Apache Spark',
      author='Chris Kirby',
      author_email='kirbycm@gmail.com',
      license='MIT',
      packages=['svm'],
      install_requires=['requests', 'colorama'],
      zip_safe=False)