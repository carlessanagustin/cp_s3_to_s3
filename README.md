# Copy from account A AWS S3 bucket to account B AWS S3 bucket

* Environment requirements

```
cd <working_folder>
sudo yum -y install python python-virtualenv python-pip
virtualenv pyenv
source pyenv/bin/activate
```

* Python environment requirements

```
pip install -r requirements.txt
```

* Run

```
python cp_s3_to_s3.py <filename>
```

> filename: must be a full path filename for each line

* Finish?

```
deactivate
```
