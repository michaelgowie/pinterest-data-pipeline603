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


