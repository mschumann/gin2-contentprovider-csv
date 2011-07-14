/*
 * ==================================================================== 
 * IQser Software License, Version 1.0 
 * Copyright (c) 2009 by Joerg Wurzer, Christian Magnus.
 * All rights reserved. Redistribution and use in source and binary
 * forms, with or without modification, are not permitted. 
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED 
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES 
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * ====================================================================
 */

package net.sf.iqser.plugin.csv;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.csvreader.CsvReader;
import com.iqser.core.event.Event;
import com.iqser.core.exception.IQserException;
import com.iqser.core.model.Attribute;
import com.iqser.core.model.Content;
import com.iqser.core.plugin.AbstractContentProvider;

/**
 * Content provider demonstrating how to access CSV files. It should be part of 
 * the file plugin package for a productive system.
 * 
 * @author Joerg Wurzer
 *
 */
public class CSVContentProvider extends AbstractContentProvider {

	/** The version */
	private static final long serialVersionUID = 8731808215080456476L;
	
	/** The logger. */
    private static Logger logger = Logger.getLogger( CSVContentProvider.class );
    
    /** The last synchronization */
    protected long lastSync = -1;
	
	/**
	 * Constructor reads the name of the CSV file
	 */
	public CSVContentProvider() {
		logger.debug( "Constructor called" );
	}

	/**
	 * Optional use in case of additional init process
	 * @see com.iqser.core.plugin.AbstractContentProvider#init()
	 */
	@Override
	public void init() {
		// Nothing to be done
	}

	/** 
	 * Optional method if any addiotnal process is required to stop the provider
	 * @see com.iqser.core.plugin.AbstractContentProvider#destroy()
	 */
	@Override
	public void destroy() {
		// Nothing to be done
	}

	/**
	 * Required method to add now or update modified content objects to the repository
	 * @see com.iqser.core.plugin.AbstractContentProvider#doSynchrinization()
	 */
	@Override
	public void doSynchonization() {
		logger.info( "doSynchronization() called" );
		
		String path = getInitParams().getProperty("csv-file");
		File source = new File(path);
	
		// Check, wether the csv-file was modified
		if (source.exists() && source.lastModified() > lastSync) {
			try {	
				char delimiter = getInitParams().getProperty("delimiter", ";").charAt(0);
				Charset charset = Charset.forName(getInitParams().getProperty("charset", "ISO-8859-1"));
				CsvReader csvReader = new CsvReader(path, delimiter, charset);
				
				csvReader.readHeaders();
				
				String urlAttrName = getInitParams().getProperty("url-attribute");
				
				while (csvReader.skipRecord()) {
					String url = csvReader.get(urlAttrName);
					
					if (isExistingContent(String.valueOf(url))) {
						updateContent(getContent(String.valueOf(url)));
					} else {
						addContent(getContent(String.valueOf(url)));
					}
				}
				csvReader.close();
			} catch (FileNotFoundException e) {
				logger.error("doSynchronization() - File not found");
			} catch (IOException e) {
				logger.error("doSynchrinization() - Can't read the file");
			} catch (IQserException e) {
				logger.error("doSynchrinization() - Can't access repository");
			}
		}
		
		lastSync = System.currentTimeMillis();

	}

	/**
	 * Required method to remove content objects from the repository
	 * @see com.iqser.core.plugin.AbstractContentProvider#doSynchrinization()
	 */
	@Override
	public void doHousekeeping() {
		logger.info( "doHousekeeping() called" );
		
		String path = getInitParams().getProperty("csv-file");
		File source = new File(path);
		
		if (source.exists() && source.lastModified() > lastSync) {
			
			try {
				char delimiter = getInitParams().getProperty("delimiter", ";").charAt(0);
				Charset charset = Charset.forName(getInitParams().getProperty("charset", "ISO-8859-1"));
				CsvReader csvReader = new CsvReader(getInitParams().getProperty("csv-file"), delimiter, charset);
				
				csvReader.readHeaders();
			
				String urlAttrName = getInitParams().getProperty("url-attribute");
				Properties urlProps = new Properties();
				
				while (csvReader.skipRecord()) {
					urlProps.setProperty(csvReader.get(urlAttrName), csvReader.getRawRecord());
				}
				
				Collection<Content> col = getExistingContents();
				Iterator<Content> iter = col.iterator();
				
				while (iter.hasNext()) {
					Content c = (Content) iter.next();
					
					if (!urlProps.containsKey(c.getContentUrl())) {
						removeContent(c.getContentUrl());
					}
				}
				
				csvReader.close();
			} catch (FileNotFoundException e) {
				logger.error("doHousekeeping() - File not found");
			} catch (IOException e) {
				logger.error("doHousekeeping() - Can't read the file");
			} catch (IQserException e) {
				logger.error("doHousekeeping() - Can't access repository");
			}

		} else if (!source.exists()) {
			
			try {
				Collection<Content> urls = getExistingContents();
				
				for (int i = 0; i < urls.size(); i++) {
					removeContent(String.valueOf(i));
				}
			} catch (IQserException e) {
				logger.error("doHousekeeping() - Can't access repository");
			}
		}
		
	}

	/**
	 * Buids the content object for a given content url respectivley line in the csv-file 
	 * @see com.iqser.core.plugin.AbstractContentProvider#getContent(java.lang.String)
	 */
	@Override
	public Content getContent(String url) {
		logger.debug( "getContent() called" );
		Content c = new Content();
		
		c.setContentUrl(url); 
		c.setProvider(getId());  // id from plugin.xml
		c.setType(getType());    // type from plugin.xml
		
		try {
			File csvfile = new File(getInitParams().getProperty("csv-file"));
			c.setModificationDate(csvfile.lastModified());
	
			char delimiter = getInitParams().getProperty("delimiter", ";").charAt(0);
			Charset charset = Charset.forName(getInitParams().getProperty("charset", "ISO-8859-1"));
			CsvReader csvReader = new CsvReader(getInitParams().getProperty("csv-file"), delimiter, charset);
			
			csvReader.readHeaders();
			csvReader.setTrimWhitespace(true);
			
			// Iterating through rows to fetch the data for the required object
			while (csvReader.skipRecord()) {
				String urlAttrName = getInitParams().getProperty("url-attribute");
				
				if (String.valueOf(csvReader.get(urlAttrName)).equalsIgnoreCase(url)) {
					int columnCount = csvReader.getHeaderCount();
					// Iterating through columns to add attributes to the content object
					for (int currentColumn = 0; currentColumn < columnCount; currentColumn++) {
						String name = csvReader.getHeader(currentColumn);
						String value = csvReader.get(currentColumn);
						int type = Attribute.ATTRIBUTE_TYPE_TEXT;
						// 1. key attribute is used in syntax analysis
						// 2. key attributes are searchable and accessible in client connector
						boolean key = getInitParams().getProperty("key-attributes", "Name").contains(name);
						
						if (value != null && value.length() > 0) {
							c.addAttribute(new Attribute(name, value, type, key));
						}
					}
					
					// 1. for the pattern analyser
					// 2. for full text search in client connector
					String fullText = csvReader.getRawRecord().replace(';',' ');
					fullText = fullText.replace('(',' ');
					fullText = fullText.replace(')',' ');
					
					c.setFulltext(fullText);
					csvReader.close();
					return c;
				}
			}
		} catch (FileNotFoundException e) {
			logger.error("getContentUrls(String) - File not found");
			return null;
		} catch (IOException e) {
			logger.error("getContentUrls(String) - Can't read the file");
			return null;
		}
		
		return null;
	}

	/* A method to deliver the complete content object, for example to desplay the content object
	 * @see com.iqser.core.plugin.ContentProvider#getBinaryData(com.iqser.core.model.Content)
	 */
	public byte[] getBinaryData(Content c) {
		logger.debug( "getBinaryData() called" );
		return c.getFulltext().getBytes();
	}

	/**
	 * This method may be used to do anything with the content objects
	 * @see com.iqser.core.plugin.AbstractContentProvider#getActions(com.iqser.core.model.Content)
	 */
	@Override
	public Collection<String> getActions(Content arg0) {
		// Not uses in this content provider
		return null;
	}

	/**
	 * Performs defined actions related a specing content object of this provider
	 * @see com.iqser.core.plugin.AbstractContentProvider#performAction(java.lang.String, com.iqser.core.model.Content)
	 */
	@Override
	public void performAction(String arg0, Content arg1) {
		// Nothing to be done, because no actions are defined.
	}

	/**
	 * This method is deprecated and was used to retrieve identifierd for objects in 
	 * the csv-file. Therfore it just parses a CSV-file which is defined in an init-param 
	 * of this plugin to read the contentUrls
	 * @see com.iqser.core.plugin.AbstractContentProvider#getContentUrls()
	 */
	@Override
	public Collection<String> getContentUrls() {
		logger.info( "getContentUrls() called" );
		
		final Collection<String> urls = new ArrayList<String>();
		try {
			char delimiter = getInitParams().getProperty("delimiter", ";").charAt(0);
			Charset charset = Charset.forName(getInitParams().getProperty("charset", "ISO-8859-1"));
			CsvReader csvReader = new CsvReader(getInitParams().getProperty("csv-file"), delimiter, charset);
			
			csvReader.readHeaders();
			
			int index = -1;
			
			while (csvReader.skipRecord()) {
				index++;
				urls.add(String.valueOf(index));
			}
			csvReader.close();
		} catch (FileNotFoundException e) {
			logger.error("getContentUrls(String) - File not found");
			return null;
		} catch (IOException e) {
			logger.error("getContentUrls(String) - Can't read the file");
			return null;
		}
		
		return urls;
	}

	/**
	 * Deprecated method to use other plugins if a content object contains sub content .
	 * @see com.iqser.core.plugin.AbstractContentProvider#getContent(java.io.InputStream)
	 */
	@Override
	public Content getContent(InputStream arg0) {
		return null;
	}

	/**
	 * Deprecated methods to use in case of using events of other plugins for exmplae 
	 * an email containing a csv file
	 * @see com.iqser.core.plugin.AbstractContentProvider#onChangeEvent(com.iqser.core.event.Event)
	 */
	@Override
	public void onChangeEvent(Event arg0) {
		// Nothing to be done
	}

}
