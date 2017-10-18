 # Serverless processing of MQ messages with IBM Cloud Functions

This repository contains a sample Java Action for processing IBM® MQ messages using IBM Cloud Functions (OpenWhisk).

For details please see [the accompanying blog post here](https://developer.ibm.com/messaging/2017/10/18/serverless-mq-processing-cloud-functions/).

The high level steps described in the blog post are;
1. Download and configure the necessary tools and sample code
  - IBM Cloud Functions CLI
  - Sample code (this project)
  - IBM MQ JMS client library
  - Internet accessible queue manager
2. Build and test the IBM Cloud Functions “Action” that will process our messages
  - use 'gradle build' after cloning this project
  - update the configuration.json file with details specific to your endpoints
3. Upload the Action to Bluemix and configure an Alarm and Rule so that it is invoked
  - Use 'wsk action create' to upload the Action to IBM Bluemix
  - Use 'wsk action update' to apply the configuration parameters
  - Configure an Alarm and Rule to cause the Action to be invoked
4. Put some sample messages to the queue to see our Action in action!
  - Use the MQ Web Console or a messaging application to send some messages
  - Observe the Action being invoked using the Bluemix user interface
