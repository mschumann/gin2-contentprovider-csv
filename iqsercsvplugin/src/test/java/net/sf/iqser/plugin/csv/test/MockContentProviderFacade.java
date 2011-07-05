package net.sf.iqser.plugin.csv.test;

import java.util.Collection;

import com.iqser.core.exception.IQserException;
import com.iqser.core.model.Content;
import com.iqser.core.plugin.ContentProviderFacade;

/**
 * Supports testing of the csv content provider.
 * 
 * @author Tim Antusch
 *
 */
public class MockContentProviderFacade implements ContentProviderFacade {

	public void addContent(Content arg0) throws IQserException {
		// TODO Auto-generated method stub
		
	}

	public Collection<Content> getExistingContents(String arg0)
			throws IQserException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isExistingContent(String arg0, String arg1)
			throws IQserException {
		return false;
	}

	public void removeContent(String arg0, String arg1) throws IQserException {
		// TODO Auto-generated method stub
		
	}

	public void updateContent(Content arg0) throws IQserException {
		// TODO Auto-generated method stub
		
	}
	
	

}