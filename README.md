# Pinterest Data Pipeline
---
## Description
The aim of this project is to create a pipeline which takes a data stream which simulates posts on a global social media platform, pinterest, extracts the data, transforms it and loads it. 
I began by configuring an AWS EC2 kafka client. To do this I connected to an EC2 client and set up the AWS IAM authentication so that I could connect a WSL terminal to an Amazon MSK cluster and the EC2 instance. Finally, I created three topics, one for the data relating to the post or pin itself, one for data relating to the user who posted and one for the data relating to where in the world the post was made from.
After this, I used MSK connect to connect the MSK cluster to an S3 bucket to store the data streamed by the Kafka client. To do this, I created a custom MSK plugin and a connector, meaning that any data which was subsequently passed through the cluster would automatically be stored in the S3 bucket.
My next task was to use the Amazon API Gateway to configure an API which would send data from the file which emulated a stream of pinterest pins, namely user_posting_emulation.py. I used confluent to send data to the Kafka client, which in turn sent it to the MSK cluster and then the S3 bucket. At this point I was ready to use the user_posting_emulation.py file and I let data be streamed through to the S3 bucket, ready to be transformed.
In order to transform the data, I set up a DataBricks notebook, and loaded the data from the S3 bucket into three dataframes, one for each of the Kafka topics, ready to be cleaned and queried. I then cleaned these three dataframes using pyspark. I removed duplicate data points, changed uninformative data into the None data type and performed functions on certain columns to make the data consistent and usable.
In the same notebook, I began to query the data, producing multiple informative insights about the massive number of posts streamed through the pipeline. I did this using SQL.
With the Notebook completed, I created a DAG and uploaded to an MWAA environment. I then scheduled it to run at midnight every night, so that the notebook will obtain any new data from the S3 bucket and clean it every day.
---
I then created a data stream pipeline with the use of Amazon kinesis and databricks. I began by increasing the functionality of the previously created API by configuring it to interact with amazon kinesis. I then created a new file, user_posting_emulation_streaming.py, which was based on the previous post emulation file, which sent the data to kinesis as opposed to S3. 
Subsequently, I created a DataBricks notebook which dealt with streaming tables to extract the data from the kinesis stream, transform and clean it and then load it to a Delta Tables stream.


## Files
In this repository, we have two DataBricks notebooks. The one entitled final-S3-connector contains the code which connects to the S3 bucket, extracts the data and queries it. The file queries.sql contains these queries as well.
We also have the 121704c19363_dag.py file which contains the DAG which runs the notebook each night. 
We then have the two files user_posting_emulation.py, which simulates user posts and sends them to the S3 bucket, and user_posting_emulation_streaming.py, which sends the posts to Amazon Kinesis.
Finally, we have the second notebook, Kinesis-Connector. This contains code which takes the kinesis stream and creates three delta tables streams with clean data. 

## Licansing
MIT License

Copyright (c) 2024 Michael Gowie

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE."


