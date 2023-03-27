# AWS Lambda function to push data to ES from S3

This function takes the input from csv file of a particular format pushed into S3. It then parses it and pushes to Elasticsearch index. The index is created based on the csv schema. THe required variables are:
- ES_URL : The URL for the running elasticsearch instance
- index: The index name where the data is to be pushed. The index can be present or needs to be created.

## Executing

If there is a change in schema of csv, the index mapping and setting value for different fields need to be changed.
To test the code upload the function to aws lambda and publish a csv file to the bucket to which it is linked.