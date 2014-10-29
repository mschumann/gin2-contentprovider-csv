package net.sf.iqser.plugin.csv;

import java.util.Collection;

import com.iqser.core.model.Content;
import com.iqser.core.model.Parameter;
import com.iqser.core.plugin.provider.ContentProvider;

public interface ActionRunner {

	void run(Collection<Parameter> parameters, Content content, ContentProvider provider);

}
