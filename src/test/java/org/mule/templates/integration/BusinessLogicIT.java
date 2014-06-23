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

	protected static final int TIMEOUT_SEC = 120;
	protected static final String TEMPLATE_NAME = "contact-migration";

	protected SubflowInterceptingChainLifecycleWrapper retrieveContactFromSFFlow;
	private List<Map<String, Object>> createdAccountsInA = new ArrayList<Map<String, Object>>(),
			createdAccountsInB = new ArrayList<Map<String, Object>>();
	private BatchTestHelper helper;

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
		deleteTestAccountsFromSandBoxA();		
		deleteTestAccountsFromSandBoxB();
	}

	@Test(timeout = TIMEOUT_SEC * 1000)
	public void testMainFlow() throws Exception {
		runFlow("mainFlow");
		
		// Wait for the batch job executed by the poll flow to finish
		helper.awaitJobTermination(TIMEOUT_SEC * 1000, 500);
		helper.assertJobWasSuccessful();
	
		Map<String, Object> payload0 = invokeRetrieveFlow(retrieveContactFromSFFlow, createdAccountsInA.get(0));
		Assert.assertNotNull("The account 0 should have been sync but is null", payload0);
		Assert.assertEquals("The account 0 should have been sync (First Name)", createdAccountsInA.get(0).get(KEY_FIRST_NAME), payload0.get(KEY_FIRST_NAME_SF));
		Assert.assertEquals("The account 0 should have been sync (Email)", createdAccountsInA.get(0).get(KEY_EMAIL).toString().toLowerCase(), payload0.get(KEY_EMAIL_SF));

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
		
		String uniqueSuffix = "_" + TEMPLATE_NAME;
		
		Map<String, Object> account_3_B = new HashMap<String, Object>();
		account_3_B.put(KEY_FIRST_NAME_SF, "Name_3_B" + uniqueSuffix);
		account_3_B.put(KEY_LAST_NAME_SF, "Name_3_B" + uniqueSuffix);		
		account_3_B.put(KEY_EMAIL_SF, "Name_3_B" + uniqueSuffix + "@gmail.com");		
		createdAccountsInB.add(account_3_B);
	
		SubflowInterceptingChainLifecycleWrapper createAccountInBFlow = getSubFlow("insertContactSF");
		createAccountInBFlow.initialise();
		MuleEvent event = createAccountInBFlow.process(getTestEvent(createdAccountsInB, MessageExchangePattern.REQUEST_RESPONSE));
		List<EnrichedSaveResult> list = (List<EnrichedSaveResult>) event.getMessage().getPayload();
		createdAccountsInB.get(0).put(KEY_ID, list.get(0).getId());
		
		Thread.sleep(1001); // this is here to prevent equal LastModifiedDate
		
		// Create accounts in source system to be or not to be synced
	
		Map<String, Object> account_0_A = new HashMap<String, Object>();
		account_0_A.put(KEY_FIRST_NAME, "Name_0_A");
		account_0_A.put(KEY_LAST_NAME, "Name_0_A");
		account_0_A.put(KEY_EMAIL, account_0_A.get(KEY_FIRST_NAME) + "@gmail.com");
		createdAccountsInA.add(account_0_A);
				

		Map<String, Object> account_1_A = new HashMap<String, Object>();
		account_1_A.put(KEY_FIRST_NAME,  "Name_updated");
		account_1_A.put(KEY_LAST_NAME, "Name_1_A");
		account_1_A.put(KEY_EMAIL, account_3_B.get(KEY_EMAIL_SF));
		createdAccountsInA.add(account_1_A);
		
		SubflowInterceptingChainLifecycleWrapper createAccountInAFlow = getSubFlow("insertContactSiebel");
		createAccountInAFlow.initialise();
		for (int i = 0; i < createdAccountsInA.size(); i++){			
			event = createAccountInAFlow.process(getTestEvent(createdAccountsInA.get(i), MessageExchangePattern.REQUEST_RESPONSE));			
			CreateResult cr = (CreateResult) event.getMessage().getPayload();
		
			// assign Siebel-generated IDs						
			createdAccountsInA.get(i).put(KEY_ID, cr.getCreatedObjects().get(0));
		}
		createdAccountsInB.get(0).put("Siebel_Id__c", createdAccountsInA.get(0).get(KEY_ID));
		System.out.println("Results after adding: " + createdAccountsInA.toString());
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

	@SuppressWarnings("uncheck" +
			"ed")
	protected Map<String, Object> invokeRetrieveFlow(SubflowInterceptingChainLifecycleWrapper flow, Map<String, Object> payload) throws Exception {
		MuleEvent event = flow.process(getTestEvent(payload, MessageExchangePattern.REQUEST_RESPONSE));
		Object resultPayload = event.getMessage().getPayload();
		return resultPayload instanceof NullPayload ? null : ((ConsumerIterator<Map<String, Object>>) resultPayload).next();
	}
	
	private void deleteTestAccountsFromSandBoxA() throws InitialisationException, MuleException, Exception {
		SubflowInterceptingChainLifecycleWrapper deleteAccountFromAFlow = getSubFlow("deleteContactSiebel");
		deleteAccountFromAFlow.initialise();
		deleteTestEntityFromSandBox(deleteAccountFromAFlow, createdAccountsInA);
	}

	private void deleteTestAccountsFromSandBoxB() throws InitialisationException, MuleException, Exception {
		SubflowInterceptingChainLifecycleWrapper deleteAccountFromBFlow = getSubFlow("deleteContactSF");
		deleteAccountFromBFlow.initialise();
		deleteTestEntityFromSandBox(deleteAccountFromBFlow, createdAccountsInB);
	}
	
	private void deleteTestEntityFromSandBox(SubflowInterceptingChainLifecycleWrapper deleteFlow, List<Map<String, Object>> entitities) throws MuleException, Exception {
		List<String> idList = new ArrayList<String>();
		for (Map<String, Object> c : entitities) {
			idList.add(c.get(KEY_ID).toString());
			System.out.println("id: " + c.get(KEY_ID).toString());
		}
		deleteFlow.process(getTestEvent(idList, MessageExchangePattern.REQUEST_RESPONSE));
	}

}
