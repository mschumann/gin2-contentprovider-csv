package net.sf.iqser.plugin.csv;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import net.sf.iqser.plugin.csv.test.MockContentProviderFacade;

import org.apache.log4j.PropertyConfigurator;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import com.iqser.core.config.Configuration;
import com.iqser.core.config.ServiceLocatorFactory;
import com.iqser.core.db.ConfigurationManager;
import com.iqser.core.exception.IQserException;
import com.iqser.core.locator.SimpleTestServiceLocator;
import com.iqser.core.model.Attribute;
import com.iqser.core.model.Content;
import com.iqser.core.plugin.provider.ContentProviderFacade;
import com.iqser.core.repository.RepositoryReader;

public class CsvContentProviderTest extends MockObjectTestCase {

	private CsvContentProvider provider;

	@Override
	protected void setUp() throws Exception {
		PropertyConfigurator.configure(System.getProperty("user.dir") + "/src/test/res/log4j.properties");

		super.setUp();

		// initialize the SimpleTestServiceLocator
		System.setProperty(ServiceLocatorFactory.COM_IQSER_SERVICE_LOCATOR,
				SimpleTestServiceLocator.class.getCanonicalName());
		SimpleTestServiceLocator serviceLocator = (SimpleTestServiceLocator) ServiceLocatorFactory.getServiceLocator();
		serviceLocator.setContentProviderFacade(new MockContentProviderFacade());


		// initialize the Configuration
		Configuration configuration = new Configuration();

		// initialize the Configuration mock object
		Mock configurationManagerMock = mock(ConfigurationManager.class);
		configurationManagerMock.stubs().method("getActiveConfiguration").withNoArguments()
		.will(returnValue(configuration));
		serviceLocator.setConfigurationManager((ConfigurationManager) configurationManagerMock.proxy());

		// initialize the RepositoryReader mock object
		Mock repositoryReaderMock = mock(RepositoryReader.class);
		repositoryReaderMock.stubs().method("contains").withAnyArguments().will(returnValue(false));
		repositoryReaderMock.stubs().method("contains").withAnyArguments().will(returnValue(false));
		serviceLocator.setRepositoryReader((RepositoryReader) repositoryReaderMock.proxy());

		Properties initParams = new Properties();
		initParams.setProperty("csv-file", System.getProperty("user.dir") + "/artcollection.csv");
		initParams.setProperty("columns.key", "1,2,3,6");
		initParams.setProperty("columns.id", "0");
		initParams.setProperty("column.idAsContentUrl", "true");
		initParams.setProperty("content.type", "Artwork");
		initParams.setProperty("recordAsFulltext", "true");

		provider = new CsvContentProvider();
		provider.setInitParams(initParams);

		provider.setName("CSV-CP");

		provider.init();
	}

	public void testDoSynchronization() {
		provider.doSynchronization();

		ContentProviderFacade contentProviderFacade = ServiceLocatorFactory.getServiceLocator()
				.getContentProviderFacade();

		try {
			Collection<Content> col = contentProviderFacade.getExistingContents("net.sf.iqser.plugin.csv");
			assertEquals(37, col.size());

			Iterator<Content> iter = col.iterator();

			while (iter.hasNext()) {
				Content c = iter.next();

				assertEquals("Artwork", c.getType());
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


	public void noTestDoHousekeeping() {
		provider.doSynchronization();

		provider.getInitParams().setProperty("csv-file",
				System.getProperty("user.dir") + "/src/test/res/artcollection.csv");
		provider.doHousekeeping();

		ContentProviderFacade cpf = ServiceLocatorFactory.getServiceLocator().getContentProviderFacade();

		try {
			Collection<Content> col = cpf.getExistingContents("net.sf.iqser.plugin.csv");
			assertEquals(37, col.size());
		} catch (IQserException e) {
			e.printStackTrace();
		}
	}

	public void testGetContentString() {
		Content c = provider.createContent("3");
		assertEquals("Alfred Mandeville", c.getAttributeByName("ARTIST").getValue());
		assertEquals(true, c.getAttributeByName("ARTIST").isKey());
		assertEquals(8, c.getAttributes().size());
	}

	public void testGetContentUrls() {
		assertEquals(37, provider.getContentUrls().size());
	}

	public void testGetBinaryData() {
		Content c = provider.createContent("3");
		String s = "3 Alfred Mandeville Tete Bleue sculpture 23 56 250,00 150,00";

		assertEquals(s, new String(provider.getBinaryData(c)));
	}

}