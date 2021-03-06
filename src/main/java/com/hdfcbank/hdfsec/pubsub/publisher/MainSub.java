package com.hdfcbank.hdfsec.pubsub.publisher;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.threeten.bp.Duration;

import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

public class MainSub {

	
//private static final Logger logger = LoggerFactory.getLogger(MainSub.class);

public static void main( String[] args ){

	String projectId="hbl-poc-digfac-anthos-prj";
	String subscriptionId="test-hsl-sub";
	new MainSub().subscribeWithConcurrencyControlExample(projectId, subscriptionId);
}

	public void subscribeWithConcurrencyControlExample(String projectId, String subscriptionId) {
		ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
		//Set<String> messageIdSet = new HashSet<>();
		
		
		// Instantiate an asynchronous message receiver.
		MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
			// Handle incoming message, then ack the received message.
			String key = message.getMessageId();
			String content = message.getData().toStringUtf8();
//			if(!messageIdSet.add(key))
//				System.out.println("Duplicate - Id: " + key +"\tData: " + content);
			//redisSevice.publishToRedis(key, content); //REDIS
			System.out.println("Id: " + key +"\tData: " + content);
//			try {
//				Thread.sleep(3000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
			consumer.ack();
		};

		Subscriber subscriber = null;
		try {
			// Provides an executor service for processing messages. The default
			// `executorProvider` used
			// by the subscriber has a default thread count of 5.
			ExecutorProvider executorProvider = InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(4)
					.build();

			// `setParallelPullCount` determines how many StreamingPull streams the
			// subscriber will open
			// to receive message. It defaults to 1. `setExecutorProvider` configures an
			// executor for the
			// subscriber to process messages. Here, the subscriber is configured to open 2
			// streams for
			// receiving messages, each stream creates a new executor with 4 threads to help
			// process the
			// message callbacks. In total 2x4=8 threads are used for message processing.
			subscriber = Subscriber.newBuilder(subscriptionName, receiver).setParallelPullCount(2)
					.setExecutorProvider(executorProvider).setMaxAckExtensionPeriod(Duration.ofHours(5)).build();

			// Start the subscriber.
			subscriber.startAsync().awaitRunning();
			System.out.println("Listening for messages on "+ subscriptionName.getSubscription()+"\n");
			// Allow the subscriber to run for 30s unless an unrecoverable error occurs.
			subscriber.awaitTerminated(20, TimeUnit.SECONDS);
		} catch (TimeoutException timeoutException) {
			// Shut down the subscriber after 30s. Stop receiving messages.
			subscriber.stopAsync();
		}
	}
}
