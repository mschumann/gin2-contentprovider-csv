package net.sf.iqser.plugin.csv.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.iqser.core.exception.IQserException;
import com.iqser.core.model.Content;
import com.iqser.core.plugin.provider.ContentProviderFacade;

public class MockContentProviderFacade implements ContentProviderFacade {

	private Collection<Content> contents = null;

	public MockContentProviderFacade() {
		contents = new ArrayList<Content>();
	}

	public void addContent(Content arg0) throws IQserException {
		contents.add(arg0);
	}

	public Collection<Content> getExistingContents(String arg0) throws IQserException {

		Collection<Content> col = new ArrayList<Content>();
		col.addAll(contents);

		return col;
	}

	public boolean isExistingContent(String arg0, String arg1) throws IQserException {
		Iterator<Content> cIter = contents.iterator();

		while (cIter.hasNext()) {
			Content c = cIter.next();

			if (c.getContentUrl().equalsIgnoreCase(arg1)) {
				return true;
			}
		}

		return false;
	}

	public void removeContent(String arg0, String arg1) throws IQserException {
		Iterator<Content> cIter = contents.iterator();

		while (cIter.hasNext()) {
			Content c = cIter.next();

			if (c.getContentUrl().equalsIgnoreCase(arg1)) {
				contents.remove(c);
				break;
			}
		}
	}

	public void updateContent(Content arg0) throws IQserException {
		Iterator<Content> cIter = contents.iterator();

		while (cIter.hasNext()) {
			Content c = cIter.next();

			if (c.getContentUrl().equalsIgnoreCase(arg0.getContentUrl())) {
				contents.remove(c);
				contents.add(arg0);
				break;
			}
		}
	}

	public Content getExistingContent(String arg0, String arg1) throws IQserException {
		// TODO Auto-generated method stub
		return null;
	}
}
