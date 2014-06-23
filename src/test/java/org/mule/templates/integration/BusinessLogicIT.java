package org.mule.templates.integration;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.MessageExchangePattern;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.config.MuleProperties;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.modules.salesforce.bulk.EnrichedSaveResult;
import org.mule.modules.siebel.api.model.response.CreateResult;
import org.mule.processor.chain.SubflowInterceptingChainLifecycleWrapper;
import org.mule.streaming.ConsumerIterator;
import org.mule.tck.junit4.FunctionalTestCase;
import org.mule.transport.NullPayload;

import com.mulesoft.module.batch.BatchTestHelper;

/**
 * The objective of this class is to validate the correct behavior of the flows
 * for this Anypoint Tempalte that make calls to external systems.
 * 
 */
public class BusinessLogicIT extends FunctionalTestCase {

	private static final String KEY_ID = "Id";
	private static final String KEY_FIRST_NAME = "First Name";	
	private static final String KEY_LAST_NAME = "Last Name";
	private static final String KEY_LAST_NAME_SF = "LastName";
	private static final String KEY_EMAIL = "Email Address";
	private static final String KEY_FIRST_NAME_SF = "FirstName";	
	private static final String KEY_EMAIL_SF = "Email";
	private static final String MAPPINGS_FOLDER_PATH = "./mappings";
	private static final String TEST_FLOWS_FOLDER_PATH = "./src/test/resources/flows/";
	private static final String MULE_DEPLOY_PROPERTIES_PATH = "./src/main/app/mule-deploy.properties";
	private static final String KEY_ACCOUNT = "Account";
	protected static final int TIMEOUT_SEC = 120;
	protected static final String TEMPLATE_NAME = "contact-migration";
	private String accountName = TEMPLATE_NAME + "-account";
	
	protected SubflowInterceptingChainLifecycleWrapper retrieveContactFromSFFlow;
	private List<Map<String, Object>> createdContactsInA = new ArrayList<Map<String, Object>>(),
			createdContactsInB = new ArrayList<Map<String, Object>>();
	private BatchTestHelper helper;
	private Map<String, Object> account = new HashMap<String, Object>(); 
	
	@Before
	public void setUp() throws Exception {
		helper = new BatchTestHelper(muleContext);
		// Flow to retrieve accounts from target system after sync in g
		retrieveContactFromSFFlow = getSubFlow("selectContactSF");
		retrieveContactFromSFFlow.initialise();
		createTestDataInSandBox();
	}

	@After
	public void tearDown() throws Exception {		
		deleteTestContactsFromSandBoxA();		
		deleteTestContactsFromSandBoxB();
	}

	@Test
	public void testMainFlow() throws Exception {
		runFlow("mainFlow");
		
		// Wait for the batch job executed by the poll flow to finish
		helper.awaitJobTermination(TIMEOUT_SEC * 1000, 500);
		helper.assertJobWasSuccessful();
	
		Map<String, Object> payload0 = invokeRetrieveFlow(retrieveContactFromSFFlow, createdContactsInA.get(0));
		Assert.assertNotNull("The contact 0 should have been sync but is null", payload0);
		Assert.assertEquals("The contact 0 should have been sync (First Name)", createdContactsInA.get(0).get(KEY_FIRST_NAME), payload0.get(KEY_FIRST_NAME_SF));
		Assert.assertEquals("The contact 0 should have been sync (Email)", createdContactsInA.get(0).get(KEY_EMAIL).toString().toLowerCase(), payload0.get(KEY_EMAIL_SF));
	
		// test if an account was created along with the contact
		MuleEvent event = getSubFlow("selectAccountSF").process(getTestEvent(createdContactsInA.get(0), MessageExchangePattern.REQUEST_RESPONSE));
		Assert.assertNotNull("The account for contact 0 should have been sync but is null", event.getMessage().getPayload());
		
		Map<String, Object>  payload1 = invokeRetrieveFlow(retrieveContactFromSFFlow, createdContactsInA.get(1));
		Assert.assertNotNull("The contact 1 should have been sync but is null", payload1);
		Assert.assertEquals("The contact 1 should have been sync (First Name)", createdContactsInA.get(1).get(KEY_FIRST_NAME), payload1.get(KEY_FIRST_NAME_SF));		
		Assert.assertEquals("The contact 1 should have been sync (Email)", createdContactsInA.get(1).get(KEY_EMAIL).toString().toLowerCase(), payload1.get(KEY_EMAIL_SF));				
	}

	@Override
	protected String getConfigResources() {
		Properties props = new Properties();
		try {
			props.load(new FileInputStream(MULE_DEPLOY_PROPERTIES_PATH));
		} catch (IOException e) {
			throw new IllegalStateException(
					"Could not find mule-deploy.properties file on classpath. " +
					"Please add any of those files or override the getConfigResources() method to provide the resources by your own.");
		}

		return props.getProperty("config.resources") + getTestFlows();
	}


	private void createTestDataInSandBox() throws MuleException, Exception {
		// Create object in target system to be updated
		
		account.put("Name", accountName);
		String uniqueSuffix = "_" + TEMPLATE_NAME;
		
		Map<String, Object> contact_3_B = new HashMap<String, Object>();
		contact_3_B.put(KEY_FIRST_NAME_SF, "Name_3_B" + uniqueSuffix);
		contact_3_B.put(KEY_LAST_NAME_SF, "Name_3_B" + uniqueSuffix);		
		contact_3_B.put(KEY_EMAIL_SF, "Name_3_B" + uniqueSuffix + "@gmail.com");		
		createdContactsInB.add(contact_3_B);
	
		SubflowInterceptingChainLifecycleWrapper createAccountInBFlow = getSubFlow("insertContactSF");
		createAccountInBFlow.initialise();
		MuleEvent event = createAccountInBFlow.process(getTestEvent(createdContactsInB, MessageExchangePattern.REQUEST_RESPONSE));
		@SuppressWarnings(value = { "unchecked" })
		List<EnrichedSaveResult> list = (List<EnrichedSaveResult>) event.getMessage().getPayload();
		createdContactsInB.get(0).put(KEY_ID, list.get(0).getId());
		
		Thread.sleep(1001); // this is here to prevent equal LastModifiedDate
		
		// Create accounts in source system to be or not to be synced
	
		Map<String, Object> contact_0_A = new HashMap<String, Object>();
		contact_0_A.put(KEY_FIRST_NAME, "Name_0_A"+ uniqueSuffix);
		contact_0_A.put(KEY_LAST_NAME, "Name_0_A"+ uniqueSuffix);
		contact_0_A.put(KEY_EMAIL, contact_0_A.get(KEY_FIRST_NAME) + "@gmail.com");
		contact_0_A.put(KEY_ACCOUNT, account.get("Name"));
		createdContactsInA.add(contact_0_A);
				

		Map<String, Object> contact_1_A = new HashMap<String, Object>();
		contact_1_A.put(KEY_FIRST_NAME,  "Name_updated");
		contact_1_A.put(KEY_LAST_NAME, "Name_1_A");
		contact_1_A.put(KEY_EMAIL, contact_3_B.get(KEY_EMAIL_SF));
		createdContactsInA.add(contact_1_A);
		
		SubflowInterceptingChainLifecycleWrapper createContactInAFlow = getSubFlow("insertContactSiebel");
		createContactInAFlow.initialise();
		for (int i = 0; i < createdContactsInA.size(); i++){			
			event = createContactInAFlow.process(getTestEvent(createdContactsInA.get(i), MessageExchangePattern.REQUEST_RESPONSE));			
			CreateResult cr = (CreateResult) event.getMessage().getPayload();
			// assign Siebel-generated IDs						
			createdContactsInA.get(i).put(KEY_ID, cr.getCreatedObjects().get(0));
		}
		System.out.println("Results after adding: " + createdContactsInA.toString());
	}

	private String getTestFlows() {
		File[] listOfFiles = new File(TEST_FLOWS_FOLDER_PATH).listFiles(new FileFilter() {
			@Override
			public boolean accept(File f) {
				return f.isFile() && f.getName().endsWith(".xml");
			}
		});
		
		if (listOfFiles == null) {
			return "";
		}
		
		StringBuilder resources = new StringBuilder();
		for (File f : listOfFiles) {
			resources.append(",").append(TEST_FLOWS_FOLDER_PATH).append(f.getName());
		}
		return resources.toString();
	}

	@Override
	protected Properties getStartUpProperties() {
		Properties properties = new Properties(super.getStartUpProperties());
		properties.put(
				MuleProperties.APP_HOME_DIRECTORY_PROPERTY,
				new File(MAPPINGS_FOLDER_PATH).getAbsolutePath());
		return properties;
	}

	@SuppressWarnings("unchecked")
	protected Map<String, Object> invokeRetrieveFlow(SubflowInterceptingChainLifecycleWrapper flow, Map<String, Object> payload) throws Exception {
		MuleEvent event = flow.process(getTestEvent(payload, MessageExchangePattern.REQUEST_RESPONSE));
		Object resultPayload = event.getMessage().getPayload();
		return resultPayload instanceof NullPayload ? null : ((ConsumerIterator<Map<String, Object>>) resultPayload).next();
	}
	
	private void deleteTestContactsFromSandBoxA() throws InitialisationException, MuleException, Exception {
		SubflowInterceptingChainLifecycleWrapper flow = getSubFlow("deleteContactSiebel");
		flow.initialise();
		deleteTestEntityFromSandBox(flow, createdContactsInA);
		
		flow = getSubFlow("selectAccountSiebel");
		flow.initialise();
		List<Map<String, Object>> resp = (List<Map<String, Object>>)flow.process(getTestEvent(account, MessageExchangePattern.REQUEST_RESPONSE)).getMessage().getPayload();
		
		List<String> idList = new ArrayList<String>();
		flow = getSubFlow("deleteAccountSiebel");
		flow.initialise();
		idList.add(resp.get(0).get("Id").toString());
		flow.process(getTestEvent(idList, MessageExchangePattern.REQUEST_RESPONSE));	
	}

	private void deleteTestContactsFromSandBoxB() throws InitialisationException, MuleException, Exception {
		SubflowInterceptingChainLifecycleWrapper flow = getSubFlow("selectAccountSF");
		flow.initialise();
		MuleEvent event = flow.process(getTestEvent(accountName, MessageExchangePattern.REQUEST_RESPONSE));	
		List<String> idList = new ArrayList<String>();
		idList.add(((HashMap<String, String>)event.getMessage().getPayload()).get("Id"));
		
		flow = getSubFlow("deleteAccountSF");
		flow.initialise();
		flow.process(getTestEvent(idList, MessageExchangePattern.REQUEST_RESPONSE));	
	}
	
	private void deleteTestEntityFromSandBox(SubflowInterceptingChainLifecycleWrapper deleteFlow, List<Map<String, Object>> entitities) throws MuleException, Exception {
		List<String> idList = new ArrayList<String>();
		for (Map<String, Object> c : entitities) {
			idList.add(c.get(KEY_ID).toString());
		}
		deleteFlow.process(getTestEvent(idList, MessageExchangePattern.REQUEST_RESPONSE));
	}

}
