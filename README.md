#  AWS Serverless Shipper - Lambda

This is an AWS Lambda function that ships logs from AWS services to Logz.io.

**Note**:
This project contains code for Python 2 and Python 3.
We urge you to use Python 3 because Python 2.7 will reach end of life on January 1, 2020.
 
[Get started with Python 3](https://github.com/logzio/logzio_aws_serverless/tree/master/python3)

#  Grok Patterns
**for the cloudwatch_text format introduced in**: (https://github.com/stoketalent/logzio_aws_serverless/commit/bf85259811a7ccadde700e3e0dcd79f79e43c13e)
({"message":)?{("type":"%{WORD:type}",)?("function":"%{WORD:module}::%{WORD:function}")?(.*,"companyId":"%{USERNAME:companyId}")?(.*,"functionName":"%{USERNAME:functionName}")?(.*,"level":"%{WORD:level}")?
