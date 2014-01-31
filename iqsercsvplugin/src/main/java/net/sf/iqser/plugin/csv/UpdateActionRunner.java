package net.sf.iqser.plugin.csv;

import java.util.Collection;

import com.iqser.core.model.Content;
import com.iqser.core.model.Parameter;
import com.iqser.core.plugin.provider.ContentProvider;

public class UpdateActionRunner implements ActionRunner {

	@Override
	public void run(Collection<Parameter> parameters, Content content, ContentProvider provider) {

		if (null != provider && provider instanceof CsvContentProvider) {
			// ... just call the updateCsv-method of the contentProvider
			((CsvContentProvider) provider).updateCsv(content);
		}
	}
}
