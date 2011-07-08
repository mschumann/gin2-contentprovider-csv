package net.sf.iqser.plugin.csv.test;

import com.iqser.core.analyzer.AnalyzerTaskStarter;
import com.iqser.core.analyzer.ContentAnalyzer;
import com.iqser.core.category.CategoryBuilder;
import com.iqser.core.category.CategoryManager;
import com.iqser.core.client.ClientFacade;
import com.iqser.core.event.EventPublisher;
import com.iqser.core.index.Index;
import com.iqser.core.locator.ServiceLocator;
import com.iqser.core.plugin.ContentProvider;
import com.iqser.core.plugin.ContentProviderFacade;
import com.iqser.core.plugin.PluginManager;
import com.iqser.core.repository.Repository;
import com.iqser.core.tracker.Tracker;

/**
 * Supports testing of the csv content provider
 * 
 * @author Jšrg Wurzer
 *
 */
public class TestServiceLocator implements ServiceLocator {
	
	/** Repository mockup for testing */
	private Repository rep = null;
	
	/** AnalyzerTaskStarter mpckup for testing */
	private AnalyzerTaskStarter ats = null;
	
	/** ContentProviderFacade mockup for testing */
	private ContentProviderFacade cpf = null;
	
	public void setAnalyzerTaskStarter(AnalyzerTaskStarter arg0) {
		ats = arg0;
	}

	public AnalyzerTaskStarter getAnalyzerTaskStarter() {
		return ats;
	}

	public CategoryBuilder getCategoryBuilder() {
		return null;
	}

	public CategoryManager getCategoryManager() {
		return null;
	}

	public ClientFacade getClientFacade() {
		return null;
	}

	public ContentAnalyzer getContentAnalyzer() {
		return null;
	}

	public ContentProvider getContentProvider(String arg0) {
		return null;
	}

	public EventPublisher getEventPublisher() {
		return null;
	}

	public Index getIndex() {
		return null;
	}

	public PluginManager getPluginManager() {
		return null;
	}
	
	public void setRepository(Repository mockup) {
		rep = mockup;
	}

	public Repository getRepository() {
		return rep;
	}

	public SecurityManager getSecurityManager() {
		return null;
	}

	public Tracker getTracker() {
		return null;
	}

	public void setContentProviderFacade(ContentProviderFacade mockup) {
		cpf = mockup;
	}
	
	public ContentProviderFacade getContentProviderFacade() {
		return cpf;
	}

}