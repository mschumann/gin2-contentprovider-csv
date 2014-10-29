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

	@Override
	public Collection<Content> getExistingContents(String arg0) throws IQserException {
		return new ArrayList<Content>(contents);
	}

	@Override
	public boolean isExistingContent(String arg0, String arg1) throws IQserException {
		Iterator<Content> cIter = contents.iterator();

		while (cIter.hasNext()) {
			Content c = cIter.next();

			if (c.getContentUrl().equals(arg1)) {
				return true;
			}
		}

		return false;
	}

	@Override
	public void removeContent(String arg0, String arg1) throws IQserException {
		Iterator<Content> cIter = contents.iterator();

		while (cIter.hasNext()) {
			Content c = cIter.next();

			if (c.getContentUrl().equals(arg1)) {
				contents.remove(c);
				break;
			}
		}
	}

	@Override
	public void addContent(Content arg0) throws IQserException {
		contents.add(arg0);
	}

	@Override
	public Content getExistingContent(String arg0, String arg1) throws IQserException {
		Iterator<Content> cIter = contents.iterator();

		while (cIter.hasNext()) {
			Content c = cIter.next();

			if (c.getContentUrl().equals(arg1)) {
				return c;
			}
		}

		return null;
	}

	@Override
	public void updateContent(Content arg0) throws IQserException {
		Iterator<Content> cIter = contents.iterator();
		while (cIter.hasNext()) {
			Content c = cIter.next();
			if (c.getContentUrl().equals(arg0.getContentUrl())) {
				contents.remove(c);
				contents.add(arg0);
				break;
			}
		}
	}
}
