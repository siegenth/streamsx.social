package com.ibm.streamsx.social.twitter;


import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.samples.patterns.TupleProducer;

/**
 * A source operator that does not receive any input streams and produces Twitter json tuples
 * using Twitter's 'GET status/sample' via the Oauth 1.1 security. Twitters description of the data 
 * "Returns a small random sample of all public statuses. The Tweets returned by the default access 
 * level are the same, so if two different clients connect to this endpoint, they will see the same Tweets."
 *
 * Refer to https://dev.twitter.com/docs/api/1.1
 * 
 */

@PrimitiveOperator(name="TwitterSampleStream", description=TwitterSampleStream.DESC, namespace="com.ibm.streamsx.social.twitter")
@OutputPorts({@OutputPortSet(description="Port that produces twitter status tuples in json fromat", cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating), @OutputPortSet(description="Optional output ports", optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
@Libraries(value={"opt/twitter_support/*"})
public class TwitterSampleStream extends TupleProducer {

	/**
	 * Thread for calling <code>produceTuples()</code> to produce tuples 
	 */
    private Thread processThread;
    String consumerKey = null;
    String consumerSecret = null;    
    String accessToken = null;        
    String accessTokenSecret = null;
    String jsonString = "jsonString";
    boolean active = false;

    @Parameter(name="jsonString", optional=false,description="Name of the rstring attribute the JSON formatted twitter will, 'jsonString' is the ")
    
	public void jsonString(String jsonString) {
    	this.jsonString = jsonString;
    }
	public String jsonString() {
		return this.jsonString;
	} 
    @Parameter(name="consumerKey", optional=false,description="Oauth security 'ConsumerKey' as specified by Twitter. ")
	public void setConsumerKey(String consumerKey) {
    	this.consumerKey = consumerKey;
    }
	public String getConsumerKey() {
		return this.consumerKey;
	} 
	
	@Parameter(name="consumerSecret", optional=false,description="Oauth security 'ConsumerSecret' specified by Twitter.")
	public void setConsumerSecret(String consumerSecret) {
    	this.consumerSecret = consumerSecret;
    }
	public String getConsumerSecret() {
		return this.consumerSecret;
	}

	// ----
	@Parameter(name="accessToken", optional=false,description=" Oauth security 'AccessToken' specified by Twitter.")
	public void setToken(String token) {
    	this.accessToken = token;
    }
	public String getToken() {
		return this.accessToken;
	}
	
	// ----
	@Parameter(name="accessTokenSecret", optional=false,description="Oauth security 'AccessTokenSecret' as described by Twitter.")
	public void setAccessTokenSecret(String secret) {
    	this.accessTokenSecret = secret;
    }
	public String getAccessTokenSecret() {
		return this.accessTokenSecret;
	}
    
    /**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    TwitterStream twitterSample;    
    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        System.out.println("initalize");    	
    	// Must call super.initialize(context) to correctly setup an operator.
        super.initialize(context);
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

        try {
        	twitterSample = new TwitterStream(this, consumerKey, consumerSecret, accessToken, accessTokenSecret);
		  } catch (InterruptedException e) {
			  postError("Failed to create instantiate TwitterSample : " + e.getMessage());
		  }                
        processThread = getOperatorContext().getThreadFactory().newThread(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            twitterSample.injectStream();
                        } catch (Exception e) {
                            postError("Operator error:" + e);
                        }                    
                    }
                    
                });
        /*
         * Set the thread not to be a daemon to ensure that the SPL runtime
         * will wait for the thread to complete before determining the
         * operator is complete.
         */
        processThread.setDaemon(false);
         
    }

    /**
     * Notification that initialization is complete and all input and output ports 
     * are connected and ready to receive and submit tuples.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void allPortsReady() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        // Start a thread for producing tuples because operator 
    	// implementations must not block and must return control to the caller.
        processThread.start();
    }
    
    /**
	 * This is being calledfrom the TwitterSample as message arrive in from 
	 * twitter. 
     */
    boolean produceTuples(String json)  {
        final StreamingOutput<OutputTuple> out = getOutput(0);
        OutputTuple tuple = out.newTuple();
        tuple.setString("jsonString", json);
        try {
			out.submit(tuple);
		} catch (Exception e) {
			postError("Submit failure : " + e.getLocalizedMessage());
			e.printStackTrace();
			return(false);  // shutdown.
		}
        return(true);  	    // continue 
    }

    /**
     * Shutdown this operator, which will interrupt the thread
     * executing the <code>produceTuples()</code> method.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    public synchronized void shutdown() throws Exception {
    	twitterSample.stopStream();
    	
        if (processThread != null) {
            processThread.interrupt();
            processThread = null;
        }
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        // TODO: If needed, close connections or release resources related to any external system or data store.

        // Must call super.shutdown()
        super.shutdown();
    }

	@Override
	protected void startProcessing() throws Exception {
        // This is important!!! you need this or the Stream is marked dead before 
        // any data is pushed down. 
        createAvoidCompletionThread();    
	}

	public void postWarning(String string) {
        Logger.getLogger(this.getClass()).warn(string);		
	}
	public void postError(String string) {
        Logger.getLogger(this.getClass()).error(string);
	}
	public void postInfo(String string) {
        Logger.getLogger(this.getClass()).info(string);
	}
	public static final String DESC = 
			"Receives twitter status messages from the status/sample Twitter streaming facility. One " +
            "message per tuple delivered in JSON format. " +
            "" +
            "You must have a Twitter account and created and application.";

	
}
