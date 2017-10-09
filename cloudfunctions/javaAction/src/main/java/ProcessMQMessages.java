/*
 * Copyright 2017 IBM Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.FileReader;
import java.io.IOException;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.ibm.mq.constants.MQPropertyIdentifiers;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;

public class ProcessMQMessages {

	// Constants used to read default parameters that are used to connect to the
	// queue manager.
	private static final String PARAM_QUEUE_NAME = "queueName";
	private static final String PARAM_PASSWORD = "password";
	private static final String PARAM_USERNAME = "username";
	private static final String PARAM_QMGR_CHANNEL_NAME = "qmgrChannelName";
	private static final String PARAM_QMGR_NAME = "qmgrName";
	private static final String PARAM_QMGR_PORT = "qmgrPort";
	private static final String PARAM_QMGR_HOST_NAME = "qmgrHostName";

	// Parameter that can be passed to have the action pre-populate some sample
	// messages for itself to read.
	private static final String PARAM_NUM_TEST_MESSAGES = "numTestMessages";

	// Property that will be included in the object that is returned to the caller.
	private static final String RETURN_MESSAGES_PROCESSED = "messagesProcessed";

	/**
	 * IBM Cloud Functions (OpenWhisk) Action that processes messages from a queue
	 * on an IBM MQ queue manager.
	 * 
	 */
	public static JsonObject main(JsonObject args) {

		Connection conn = null;

		JsonObject response = new JsonObject();
		int messagesProcessed = 0;

		try {

			// Set up the connection factory to connect to the queue manager,
			// populating it with all the properties we have been provided.
			MQConnectionFactory cf = new MQQueueConnectionFactory();
			cf.setTransportType(WMQConstants.WMQ_CM_CLIENT);
			cf.setHostName(getParamAsString(PARAM_QMGR_HOST_NAME, args));
			cf.setPort(getParamAsInt(PARAM_QMGR_PORT, args));
			cf.setQueueManager(getParamAsString(PARAM_QMGR_NAME, args));
			cf.setChannel(getParamAsString(PARAM_QMGR_CHANNEL_NAME, args));

			// Create the connection to the queue manager
			conn = cf.createConnection(
					getParamAsString(PARAM_USERNAME, args), 
					getParamAsString(PARAM_PASSWORD, args));

			// Create a session and a queue object to enable us to interact with the
			// queue manager. By creating a transactional session we can roll back
			// the message onto the queue if the processing fails.
			Session session = conn.createSession(true, 0);
			Queue q = session.createQueue(getParamAsString(PARAM_QUEUE_NAME, args));

			// For testing purposes we can put some messages onto the queue using this
			// function is the caller has specified the appropriate parameter/value.
			// In reality the messages would be placed onto the queue by a separate
			// application.
			if (args.has(PARAM_NUM_TEST_MESSAGES)) {
				putTestMessages(session, q, args);
			}

			// Set up the consumer to read from the queue
			MessageConsumer consumer = session.createConsumer(q);

			// Don't forget to start the connection before trying to receive messages!
			conn.start();

			Message receivedMsg = null;

			try {
				
				// Read messages from the queue and process them until there
				// are none left to read.
				while ((receivedMsg = consumer.receiveNoWait()) != null) {

					if (receivedMsg != null) {
						
						// We found a message - hand off to a separate method to
						// carry out the necessary business logic.
						processMessage(receivedMsg);

						// Since we returned from processing the message without
						// an exception being thrown we have successfully
						// processed the message, so increment our success count
						// and commit the transaction so that the message is
						// permanently removed from the queue.
						messagesProcessed++;
						session.commit();
					}
					
				}
			
			} catch (JMSException jmse2)
			{
				// This block catches any JMS exceptions that are thrown during
				// the business processing of the message.
				jmse2.printStackTrace();
				
				// Roll the transaction back so that the message can be put back
				// onto the queue ready to have its proessing retried next time
				// the action is invoked.
				session.rollback();
				throw new RuntimeException(jmse2);
				
			} catch (RuntimeException e) {
				e.printStackTrace();
				
				// Roll the transaction back so that the message can be put back
				// onto the queue ready to have its proessing retried next time
				// the action is invoked.
				session.rollback();
				throw e;
			}

			// Indicate to the caller how many messages were successfully processed.
			response.addProperty(RETURN_MESSAGES_PROCESSED, messagesProcessed);
			return response;
	
		} catch (JMSException jmse) {
			// This block catches any JMS exceptions that are thrown before the
			// message is retrieved from the queue, so we don't need to worry
			// about rolling back the transaction.
			jmse.printStackTrace();
			
			// Pass an indication about the error back to the caller.
			response.addProperty("error", jmse.toString());
			throw new RuntimeException(jmse);

		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (JMSException jmse2) {
					// Swallow final errors.
				}
			}
		}

	}// main(JsonObject)

	/**
	 * Insert the business logic that needs to be applied to process the message
	 * into this method.
	 * 
	 * @param receivedMsg JMS message to be processed
	 * @throws RuntimeException if there is a failure in the business processing
	 *     logic.
	 * @throws JMSException
	 */
	private static void processMessage(Message receivedMsg) throws JMSException {
		TextMessage rcvTxtMsg = (TextMessage) receivedMsg;
		String txt = rcvTxtMsg.getText();
		String messageId = rcvTxtMsg.getJMSMessageID();

		// Check whether this message has previously failed processing.
		int deliveryCount = receivedMsg.getIntProperty(MQPropertyIdentifiers.MQ_JMSX_DELIVERY_COUNT);
		if (deliveryCount > 1) {
			System.out.println("WARNING: Message " + messageId + " has previously failed processing "
					+ (deliveryCount - 1) + " times.");
		}

		if (deliveryCount < 3) {

			// For testing purposes, simulate a poison message if the message we have
			// received
			// contains this specific text.
			if (txt.contains("POISON MESSAGE!")) {
				throw new RuntimeException("Simulated failure triggered!");
			}

			// TODO - insert your business logic here to process the message!
			System.out.println("Successfully processed message " + messageId + " text=" + txt);

		} else {

			// The message has failed processing multiple times over a sustained
			// period of time so is potentially a "poison" message, or else there is
			// a non-transient problem with the business processing.
			//
			// To avoid the message blocking the queue indefinitely you can decide
			// to implement some alternative processing here.

			// In this case we let the method complete successfully without doing any
			// processing, which effectively causes it to be discarded.
			System.out.println("WARNING: Discarding poison message " + messageId + " text=" + txt);

		}

	}

	/**
	 * Helper method to put some sample messages onto the queue for testing
	 * purposes.
	 * 
	 * @param session
	 * @param q Queue to which test messages should be sent
	 * @param args JSONObject containing the passed parameters
	 * @throws JMSException
	 */
	private static void putTestMessages(Session session, Queue q, JsonObject args) throws JMSException {
		int numTestMessages = getParamAsInt(PARAM_NUM_TEST_MESSAGES, args);

		MessageProducer sender = session.createProducer(q);

		// Put as many test messages as the caller has requested.
		for (int i = 1; i <= numTestMessages; i++) {

			// Send a sample message
			TextMessage msg = session.createTextMessage();
			msg.setText("SampleMessage" + i + ": " + new Date());
			sender.send(msg);
		}

		session.commit();

		System.out.println("Sent " + numTestMessages + " test messages.");
	}

	/**
	 * J2SE "main" method to allow the Action to be tested locally without having
	 * to upload to Bluemix every time we make a change.
	 * 
	 * Takes a single optional parameter which is the number of test messages to
	 * send. Default value of zero test messages if the parameter is not specified.
	 */
	public static void main(String[] args) {
		System.out.println("Simulating execution of Action");

		try {
			// Load the default parameters (hostname, port etc) from the
			// local file system.
			JsonParser parser = new JsonParser();
			JsonElement element = parser.parse(new FileReader("configuration.json"));
			JsonObject params = element.getAsJsonObject();

			// Check whether the argument was passed in to send test messages.
			if (args.length > 1) {
				params.addProperty(PARAM_NUM_TEST_MESSAGES, Integer.parseInt(args[1]));
			}

			// Simulate invoking the Action by calling the other main method in the 
			// same way as IBM Cloud Functions/OpenWhisk does.
			JsonObject retObj = main(params);
			System.out.println(retObj);

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

		System.out.println("End of simulation.");

	}// main(String[])

	/**
	 * Retrieve a string parameter from the supplied arguments or throw an
	 * exception if that parameter has not been provided.
	 * 
	 * @param name Name of the property to return
	 * @param args JsonObject containing the set of parameters
	 * @return String containing the parameter value if found
	 * @throws IllegalStateException If the parameter is not found
	 */
	private static String getParamAsString(String name, JsonObject args) {

		if (args.has(name)) {
			return args.getAsJsonPrimitive(name).getAsString();
		} else {
			throw new IllegalStateException("Parameter '" + name + "' was not found.");
		}

	}

	/**
	 * Retrieve an int parameter from the supplied arguments or throw an
	 * exception if that parameter has not been provided.
	 * 
	 * @param name Name of the property to return
	 * @param args JsonObject containing the set of parameters
	 * @return String containing the parameter value if found
	 * @throws IllegalStateException If the parameter is not found
	 */
	private static int getParamAsInt(String name, JsonObject args) {

		if (args.has(name)) {
			return args.getAsJsonPrimitive(name).getAsInt();
		} else {
			throw new IllegalStateException("Parameter '" + name + "' was not found.");
		}

	}

}
