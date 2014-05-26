package com.ibm.streamsx.social.twitter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
/** 
 * This utilizes Twitter's 'stream' interface to fetch data
 * from twitter, this is the Twitter side of the equations. 
 * 
 * @author siegentha
 *
 */
public class TwitterStream {
	BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
	BasicClient client = null;
	TwitterSampleStream twitterSampleInject;
	Authentication auth;
	StatusesSampleEndpoint endpoint;

	public TwitterStream(TwitterSampleStream twitterSampleInject,
			String consumerKey, String consumerSecret, String token,
			String tokenSecret) throws InterruptedException {
		this.twitterSampleInject = twitterSampleInject;

		endpoint = new StatusesSampleEndpoint();
		endpoint.stallWarnings(false);
		auth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret); 
		client = new ClientBuilder().name("sampleExampleClient")
				.hosts(Constants.STREAM_HOST).endpoint(endpoint)
				.authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();
	}

	public void injectStream() {

		int timeoutCount = 0;
		final int timeoutCountMax = 20;
		int timeoutCountSecs = 5;

		client.connect();
		twitterSampleInject.postInfo("client-name:" + client.getName());
		
		boolean readTwitterStream = true;
		// Do whatever needs to be done with messages
		while (readTwitterStream) {
			if (client.isDone()) {
				twitterSampleInject.postError("Client connection closed unexpectedly: "
						+ client.getExitEvent().getMessage());
				readTwitterStream = false;
				break;
			}
			String msg = null;
			try {
				msg = queue.poll(timeoutCountSecs, TimeUnit.SECONDS);
			} catch (InterruptedException e1) {
				twitterSampleInject.postError("Error queue.poll : "
						+ e1.getMessage());
				e1.printStackTrace();
				msg = null;
			}
			if (msg == null) {
				twitterSampleInject.postWarning("Did not receive a message in '"
						+ timeoutCountSecs + "' seconds.");
				if (timeoutCount++ > timeoutCountMax) {
					readTwitterStream = false;
				}
			} else {
				timeoutCount = 0;
				readTwitterStream = twitterSampleInject.produceTuples(msg);
			}
		}
		stopStream(); // shutdown if we leave loop.
	}

	public void stopStream() {
		if (!client.isDone()) {
			twitterSampleInject
					.postWarning("Shutting down the client connection : "
							+ client.getExitEvent());
			client.stop();
		} else
			twitterSampleInject
					.postWarning("Shutting down the client connection AGAIN!");
	}

}