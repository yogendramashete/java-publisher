package com.hdfcbank.hdfsec.pubsub.publisher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;


public class MainPub 
{
    public static void main( String[] args ) throws IOException, ExecutionException, InterruptedException{

    	String projectId="hbl-poc-digfac-anthos-prj";
    	String topicId="test-hsl";
    	new MainPub().publishWithConcurrencyControlExample(projectId, topicId);
    }
    
    public void publishWithConcurrencyControlExample(String projectId, String topicId)
			throws IOException, ExecutionException, InterruptedException {
		TopicName topicName = TopicName.of(projectId, topicId);
		Publisher publisher = null;
		List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

		try {
			// Provides an executor service for processing messages. The default
			// `executorProvider` used by the publisher has a default thread count of
			// 5 * the number of processors available to the Java virtual machine.
			ExecutorProvider executorProvider = InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(4)
					.build();

			// `setExecutorProvider` configures an executor for the publisher.
			publisher = Publisher.newBuilder(topicName).setExecutorProvider(executorProvider).build();

			// schedule publishing one message at a time : messages get automatically
			// batched
			for (int i = 0; i < 100; i++) {
				String message = "message " + i;
				ByteString data = ByteString.copyFromUtf8(message);
				PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

				//if (i%20==0) Thread.sleep(1000);
				// Once published, returns a server-assigned message id (unique within the
				// topic)
				ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
				messageIdFutures.add(messageIdFuture);
			}
		} finally {
			// Wait on any pending publish requests.
			List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();

			System.out.println("Published " + messageIds.size() + " messages with concurrency control.");

			if (publisher != null) {
				// When finished with the publisher, shutdown to free up resources.
				publisher.shutdown();
				publisher.awaitTermination(1, TimeUnit.MINUTES);
			}
		}
	}
}
