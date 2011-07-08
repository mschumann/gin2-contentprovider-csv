package net.sf.iqser.plugin.csv;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.log4j.PropertyConfigurator;

import com.iqser.core.config.Configuration;
import com.iqser.core.exception.IQserException;
import com.iqser.core.model.Attribute;
import com.iqser.core.model.Content;
import com.iqser.core.plugin.ContentProviderFacade;
import net.sf.iqser.plugin.csv.test.MockContentProviderFacade;
import net.sf.iqser.plugin.csv.test.TestServiceLocator;

public class CSVContentProviderTest extends TestCase {
	
	private CSVContentProvider provider;

	protected void setUp() throws Exception {
		PropertyConfigurator.configure(
				System.getProperty("user.dir") + "/src/test/res/log4j.properties");
				
		super.setUp();
		
		Properties initParams = new Properties();
		initParams.setProperty(
				"csv-file", System.getProperty("user.dir") + "/artcollection.csv");
		initParams.setProperty("key-attributes", "ARTIST TITLE MEDIUM Guide");
		initParams.setProperty("url-attribute", "NO");
		
		provider = new CSVContentProvider();
		provider.setInitParams(initParams);
		provider.setId("net.sf.iqser.plugin.csv");
		provider.setType("Artwork");
		
		Configuration.configure(new File(
				System.getProperty("user.dir") + "/src/test/res/iqser-config.xml"));
		
		TestServiceLocator sl = (TestServiceLocator)Configuration.getConfiguration().getServiceLocator();
		sl.setContentProviderFacade(new MockContentProviderFacade());
	}
	
	public void testDoSynchronization() {
		provider.doSynchonization();
		
		ContentProviderFacade cpf = Configuration.getConfiguration().getServiceLocator().getContentProviderFacade();
		
		try {
			Collection<Content> col = cpf.getExistingContents("com.iqser.training.csv.plugin");
			assertEquals(38, col.size());
			
			Iterator<Content> iter = col.iterator();
			
			while (iter.hasNext()) {
				Content c = (Content) iter.next();
				
				assertEquals("Artwork", c.getType());
				assertEquals(provider.getId(), c.getProvider());
				assertNotNull(c.getContentUrl());
				assertNotNull(c.getModificationDate());
				
				if (c.getContentUrl().equalsIgnoreCase("0")) {
					assertEquals(8, c.getAttributes().size());
					Attribute a = c.getAttributeByName("TITLE");
					assertEquals("Irises", a.getValue());
					assertEquals(Attribute.ATTRIBUTE_TYPE_TEXT, a.getType());
					assertTrue(a.isKey());
				}
			}
		} catch (IQserException e) {
			e.printStackTrace();
		}
	}
	
	public void testDoHousekeeping() {
		provider.doSynchonization();
		
		provider.getInitParams().setProperty(
				"csv-file", 
				System.getProperty("user.dir") + "/src/test/res/artcollection.csv");
		provider.lastSync = -1;
		
		provider.doHousekeeping();
		
		ContentProviderFacade cpf = Configuration.getConfiguration().getServiceLocator().getContentProviderFacade();
		
		try {
			Collection<Content> col = cpf.getExistingContents("com.iqser.training.csv.plugin");
			assertEquals(37, col.size());
		} catch (IQserException e) {
			e.printStackTrace();
		}
	}

	public void testGetContentString() {
		Content c = provider.getContent("3");
		assertEquals("Alfred Mandeville",c.getAttributeByName("ARTIST").getValue());
		assertEquals(true, c.getAttributeByName("ARTIST").isKey());
		assertEquals(8, c.getAttributes().size());
	}

	public void testGetContentUrls() {
		assertEquals(38, provider.getContentUrls().size());
	}

	public void testGetBinaryData() {
		Content c = provider.getContent("3");
		String s ="3 Alfred Mandeville Tete Bleue sculpture 23 56 £	250,00 £	150,00";
		
		assertEquals(s, new String(provider.getBinaryData(c)));
	}
	
}