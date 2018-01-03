# svm: Spark Version Manager
Manage Apache Spark installations with a simplified CLI.

# Installation
1. `pip install svm`
2. Set SPARK_HOME environmental variable to ~/.svm/active_spark  

# Usage
List official Spark versions

`svm --list`

Install specific version

`svm install 2.2.1`

Activate specified version (must be installed first)

`svm activate 2.2.1`

Disable 

# How this works
svm installs Spark versions into the local user's home directory in .svm folder. svm then symlinks .svm/active_spark to the version directory you want active.