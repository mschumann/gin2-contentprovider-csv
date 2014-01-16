package net.sf.iqser.plugin.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import net.sf.iqser.plugin.csv.test.MockContentProviderFacade;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.iqser.core.config.ServiceLocatorFactory;
import com.iqser.core.exception.IQserException;
import com.iqser.core.model.Attribute;
import com.iqser.core.model.Content;
import com.iqser.core.plugin.provider.ContentProviderFacade;
import com.iqser.gin.developer.test.TestServiceLocator;
import com.iqser.gin.developer.test.plugin.provider.ContentProviderTestCase;

public class CsvContentProviderTest extends ContentProviderTestCase {

	private final Logger LOGGER = Logger.getLogger(CsvContentProviderTest.class);
	private CsvContentProvider provider;

	@Before
	public void setUp() throws Exception {
		TestServiceLocator serviceLocator = (TestServiceLocator) ServiceLocatorFactory.getServiceLocator();
		serviceLocator.setContentProviderFacade(new MockContentProviderFacade());

		PropertyConfigurator.configure(getClass().getClassLoader().getResource("log4j.properties"));

		EasyMock.expect(repositoryReaderMock.contains(EasyMock.<String> anyObject(), EasyMock.eq("CSV-CP")))
				.andReturn(false).anyTimes();

		CsvContentProvider.clearCache();
		provider = initializeProviderUnderTest(prepareInitParams());
	}

	private CsvContentProvider initializeProviderUnderTest(Properties initParams) {
		CsvContentProvider provider = new CsvContentProvider();
		provider.setInitParams(initParams);
		provider.setName("CSV-CP");
		provider.init();
		return provider;
	}

	private Properties prepareInitParams() {
		Properties initParams = new Properties();
		initParams.setProperty("file", getClass().getClassLoader().getResource("artcollection.csv").toString());
		initParams.setProperty("columns.key", "1,2,3,6");
		initParams.setProperty("columns.id", "0");
		initParams.setProperty("column.idAsContentUrl", "true");
		initParams.setProperty("content.type", "Artwork");
		initParams.setProperty("recordAsFulltext", "true");
		return initParams;
	}

	@Test
	public void testDoSynchronization() {

		prepare();
		provider.doSynchronization();
		verify();

		ContentProviderFacade contentProviderFacade = ServiceLocatorFactory.getServiceLocator()
				.getContentProviderFacade();

		try {
			Collection<Content> col = contentProviderFacade.getExistingContents("net.sf.iqser.plugin.csv");
			assertEquals(37, col.size());

			Iterator<Content> iter = col.iterator();

			boolean foundContentUrl = false;

			while (iter.hasNext()) {
				Content c = iter.next();

				assertEquals("Artwork", c.getType());
				assertNotNull(c.getContentUrl());
				assertNotNull(c.getModificationDate());

				if (c.getContentUrl().equals("1")) {
					assertEquals(8, c.getAttributes().size());
					Attribute a = c.getAttributeByName("TITLE");
					assertEquals("Irises", a.getValue());
					assertEquals(Attribute.ATTRIBUTE_TYPE_TEXT, a.getType());
					assertTrue(a.isKey());

					foundContentUrl = true;
				}
			}

			assertTrue(foundContentUrl);

		} catch (IQserException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testDoHousekeeping() throws IQserException {

		EasyMock.expect(repositoryReaderMock.getContentByProvider(provider.getName(), false)).andReturn(
				provider.getContentMap().values());

		prepare();
		provider.doSynchronization();

		provider.getInitParams().setProperty("file",
				getClass().getClassLoader().getResource("artcollection-modified.csv").toString());
		provider.init();

		provider.doHousekeeping();
		verify();

		ContentProviderFacade cpf = ServiceLocatorFactory.getServiceLocator().getContentProviderFacade();

		try {
			Collection<Content> col = cpf.getExistingContents("net.sf.iqser.plugin.csv");
			assertEquals(35, col.size());
		} catch (IQserException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testGetContentString() {
		Content c = provider.createContent("3");
		assertEquals("Alfred Mandeville", c.getAttributeByName("ARTIST").getValue());
		assertEquals(true, c.getAttributeByName("ARTIST").isKey());
		assertEquals(8, c.getAttributes().size());
	}

	@Test
	public void testGetContentUrls() {
		assertEquals(37, provider.getContentUrls().size());
	}

	@Test
	public void testGetBinaryData() {
		Content c = provider.createContent("3");
		String s = "3 Alfred Mandeville Tete Bleue sculpture 23 56 250,00 150,00";

		assertEquals(s, new String(provider.getBinaryData(c)));
	}

	@Test
	public void testUpdateCsv() throws IOException, URISyntaxException, IQserException {
		File originalFile = new File(getClass().getClassLoader().getResource("artcollection.csv").toURI());
		File tempCsvFile = File.createTempFile("content-provider-test-", ".csv");
		copyFileUsingStream(originalFile, tempCsvFile);

		LOGGER.info("Test file: " + tempCsvFile.getAbsolutePath());
		provider.getInitParams().setProperty("file", tempCsvFile.getAbsolutePath());
		provider.init();

		Content c1 = provider.createContent("5");

		Attribute a = c1.getAttributeByName("ARTIST");
		Assert.assertFalse("Bruce McLean".equals(a.getValue()));
		a.setValue("Bruce McLean");

		ContentProviderFacade contentProviderFacade = ServiceLocatorFactory.getServiceLocator()
				.getContentProviderFacade();

		prepare();
		provider.doSynchronization();
		provider.performAction(CsvContentProvider.Action.UPDATE.name(), null, c1);
		verify();

		Content c2 = contentProviderFacade.getExistingContent(provider.getName(), "5");
		Assert.assertTrue("Bruce McLean".equals(c2.getAttributeByName("ARTIST").getValue()));
	}

	private static void copyFileUsingStream(File source, File dest) throws IOException {
		InputStream is = null;
		OutputStream os = null;
		try {
			is = new FileInputStream(source);
			os = new FileOutputStream(dest);
			byte[] buffer = new byte[1024];
			int length;
			while ((length = is.read(buffer)) > 0) {
				os.write(buffer, 0, length);
			}
		} finally {
			is.close();
			os.close();
		}
	}
}