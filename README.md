# python-kcl-app


## Create and activate virtualenv

```
~/kcl-app  ᐅ virtualenv ./
Using base prefix '/usr/local/Cellar/python3/3.5.1/Frameworks/Python.framework/Versions/3.5'
New python executable in /Users/jason/kcl-app/bin/python3.5
Also creating executable in /Users/jason/kcl-app/bin/python
Installing setuptools, pip, wheel...done.
~/kcl-app  ᐅ source bin/activate
```

## Installing

```
(kcl-app) ~/kcl-app  ᐅ pip3 install -r requirements.txt
```

## Configuring AWS Credentials

The KCL docs mention [configuring enviroment to allow use of AWS Security Credentials](https://github.com/awslabs/amazon-kinesis-client-python#before-you-get-started), however the documentation around that appears to be out of date with how [boto's docs]() recommend to set AWS Security Credentials.

> Your credentials can be passed into the methods that create connections. Alternatively, boto will check for the existence of the following environment variables to ascertain your credentials:

```
AWS_ACCESS_KEY_ID - Your AWS Access Key ID
AWS_SECRET_ACCESS_KEY - Your AWS Secret Access Key
```

You can verify `boto` is able to connect and authenticate with AWS by running the following in python:

```
In [1]: import boto

In [2]: boto.set_stream_logger('boto')

In [3]: s3 = boto.connect_s3()
2016-04-12 13:56:40,401 boto [DEBUG]:Using access key found in environment variable.
2016-04-12 13:56:40,401 boto [DEBUG]:Using secret key found in environment variable.
```

If you get an error about NoAuthHandlerFound, boto was unable to read the environment variables mentioned above.

## Running

```
`amazon_kclpy_helper.py --print_command --java $JAVA_HOME/bin/java --properties main.properties`
```

## Logging

Currently logging is setup to write to a file `app.log`

