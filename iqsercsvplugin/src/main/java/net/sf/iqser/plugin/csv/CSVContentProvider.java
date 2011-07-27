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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
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
 * @author Kresnadi Budisantoso
 * 
 */
public class CSVContentProvider extends AbstractContentProvider {

	/** The version */
	private static final long serialVersionUID = 8731808215080456476L;

	/** The logger. */
	private static Logger logger = Logger.getLogger(CSVContentProvider.class);

	private static final String CSV_PROPERTY_FILE = "file";
	private static final String CSV_PROPERTY_DELIMETER = "delimeter";
	private static final String CSV_PROPERTY_CHARSET = "charset";
	private static final String CSV_PROPERTY_IDCOLUMN = "column.id";
	private static final String CSV_PROPERTY_IDASCONTENTURL = "column.idAsContentUrl";
	private static final String CSV_PROPERTY_IDCOLUMNS = "columns.id";
	private static final String CSV_PROPERTY_KEYCOLUMNS = "columns.key";
	private static final String CSV_PROPERTY_IGNORECOLUMNS = "columns.ignore";
	private static final String CSV_PROPERTY_FULLTEXTCOLUMN = "column.fulltext";

	// for backward compatibility
	private static final String CSV_PROPERTY_FILE_OBSOLETE = "csv-file";
	private static final String CSV_PROPERTY_IDCOLUMN_OBSOLETE = "url-attribute";
	private static final String CSV_PROPERTY_KEYCOLUMNS_OBSOLETE = "key-attributes";

	public static final String CSV_DEFAULT_DELIMETER = ";";
	public static final String CSV_DEFAULT_CHARSET = "UTF-8";
	public static final String CSV_DEFAULT_IDCOLUMN = "0";
	public static final String CSV_DEFAULT_IDASCONTENTURL = "false";
	public static final String CSV_DEFAULT_KEYCOLUMNS = "";
	public static final String CSV_DEFAULT_IGNORECOLUMNS = "";
	public static final String CSV_DEFAULT_FULLTEXTCOLUMN = "-1";

	public static final String CSV_CONTENT_URI_BASE = "iqser://iqsercsvplugin.sf.net/";

	/** The settings (set by init params). */
	private Properties settings;

	/** The csv file. */
	private File file;

	/** The delimeter character of the csv file. */
	private char delimeter;

	/** Zero-based list of columns that will be used as composite id. */
	private List<Integer> idColumns;
	/**
	 * If TRUE, the value of the ID column will be used as ContentUrl, else an
	 * artificial URI will be generated.
	 */
	private boolean idAsContentUrl;

	/** The zero-based number of the column that holds the fulltext part. */
	private int fulltextColumn;

	/** Zero-based list of columns that will be used as keys. */
	private List<Integer> keyColumns;
	private boolean keyColumnCompatibilityMode = false;

	/** Zero-based list of columns that will be ignored. */
	private List<Integer> ignoreColumns;

	/** The timestamp of last modification of the csv file */
	private long modificationTimestamp;

	/**
	 * A boolean flag which indicates whether the file has been modified or not.
	 */
	private boolean modified;

	/** A map of content objects with the content's ContentUrl as keys. */
	private Map<String, Content> contentMap;

	/** The charset of the csv file. */
	private Charset charset;

	/** The last synchronization */
	// protected long lastSync = -1;

	/*
	 * This method has to be overridden because it does not use generics for
	 * type-safe casting in the parent class.
	 */
	@Override
	public Collection<Content> getExistingContents() {
		logger.info(String.format("Invoking %s#getExistingContents() ...", this
				.getClass().getSimpleName()));

		Set<Content> returnCollection = new HashSet<Content>();

		try {
			for (Object o : super.getExistingContents()) {
				if (o instanceof Content) {
					returnCollection.add((Content) o);
				}
			}
		} catch (IQserException e) {
			logger.error(
					"Error occured while trying to get all existing content objects!",
					e);
		}

		return returnCollection;
	}

	/*
	 * Returns a set of existing ContentUrls.
	 */
	private Set<String> getExistingContentUrls() {
		Set<String> existingContentUrls = new HashSet<String>();
		for (Content content : this.getExistingContents()) {
			existingContentUrls.add(content.getContentUrl());
		}

		return existingContentUrls;
	}

	/**
	 * Initializes the content provider.
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#init()
	 */
	@Override
	public void init() {
		logger.info(String.format("Invoking %s#init() ...", this.getClass()
				.getSimpleName()));

		this.settings = getInitParams();

		// Setting the file's name.
		String filename = this.settings.getProperty(CSV_PROPERTY_FILE, "")
				.trim();
		if ("".equals(filename)) {
			filename = this.settings.getProperty(CSV_PROPERTY_FILE_OBSOLETE);
			if (null == filename || "".equals(filename.trim())) {
				logger.error("No csv file specified in configuration!");
				filename = null;
			}
		}

		// Setting the file
		if (null != filename && !"".equals(filename.trim())) {

			URI fileUri = null;
			try {
				fileUri = new URI(filename);
				logger.info("CSV filename: " + filename);
				logger.info("URI of CSV file: " + fileUri
						+ ". URI is absolute? " + fileUri.isAbsolute());
			} catch (URISyntaxException e) {
				if (logger.isDebugEnabled()) {
					logger.warn(filename + " is no vaild URI.", e);
				} else {
					logger.warn(filename + " is no vaild URI.");
				}
			}

			if (null == fileUri || !fileUri.isAbsolute()) {
				this.file = new File(filename);
			} else {
				this.file = new File(fileUri.getPath());
			}
		}

		// Setting the file's delimeter.
		this.delimeter = this.settings
				.getProperty(CSV_PROPERTY_DELIMETER, CSV_DEFAULT_DELIMETER)
				.trim().charAt(0);

		// Setting the file's charset.
		String charsetProperty = this.settings.getProperty(
				CSV_PROPERTY_CHARSET, CSV_DEFAULT_CHARSET);
		try {
			this.charset = Charset.forName(charsetProperty);
		} catch (IllegalCharsetNameException e) {
			this.charset = Charset.defaultCharset();
			logger.warn(String
					.format("'%s' is an illegal chaset name. Default charset (%s) of the JVM will be used.",
							charsetProperty, charset.displayName()));
		}

		// Setting the zero-based list of numbers of the id-columns.
		String idColumnsProperty = this.settings
				.getProperty(CSV_PROPERTY_IDCOLUMNS);
		if (null == idColumnsProperty || "".equals(idColumnsProperty.trim())) {
			idColumnsProperty = this.settings.getProperty(
					CSV_PROPERTY_IDCOLUMN, CSV_DEFAULT_IDCOLUMN);
		}
		String[] idColumnStrings = idColumnsProperty.split(",");
		this.idColumns = new ArrayList<Integer>();
		for (int i = 0; i < idColumnStrings.length; i++) {
			try {
				if (!(null == idColumnStrings[i] || ""
						.equals(idColumnStrings[i].trim()))) {
					this.idColumns.add(i,
							Integer.parseInt(idColumnStrings[i].trim()));
				}
			} catch (NumberFormatException e) {
				logger.error("Could not identify id column for value: '"
						+ idColumnStrings[i]
						+ "'\nList of id columns is corrupted!", e);
				throw e;
			}
		}

		// Setting the boolean idAsContentUrl-flag.
		this.idAsContentUrl = Boolean.parseBoolean(settings.getProperty(
				CSV_PROPERTY_IDASCONTENTURL, CSV_DEFAULT_IDASCONTENTURL));

		// Setting the zero-based number of the fulltext-column.
		this.fulltextColumn = Integer.parseInt(this.settings.getProperty(
				CSV_PROPERTY_FULLTEXTCOLUMN, CSV_DEFAULT_FULLTEXTCOLUMN));

		// Setting the key columns.
		String[] keyColumnStrings = this.settings.getProperty(
				CSV_PROPERTY_KEYCOLUMNS, CSV_DEFAULT_KEYCOLUMNS).split(",");
		this.keyColumns = new ArrayList<Integer>();
		for (int i = 0; i < keyColumnStrings.length; i++) {
			try {
				if (!(null == keyColumnStrings[i] || ""
						.equals(keyColumnStrings[i].trim()))) {
					this.keyColumns.add(i,
							Integer.parseInt(keyColumnStrings[i].trim()));
				}
			} catch (NumberFormatException e) {
				logger.error("Could not identify key column for value: '"
						+ keyColumnStrings[i]
						+ "'\nList of key columns is corrupted!", e);
				throw e;
			}
		}

		// Setting the columns that will be ignored while creating the content
		// objects.
		String[] ignoreColumnStrings = this.settings.getProperty(
				CSV_PROPERTY_IGNORECOLUMNS, CSV_DEFAULT_IGNORECOLUMNS).split(
				",");
		this.ignoreColumns = new ArrayList<Integer>();
		for (int i = 0; i < ignoreColumnStrings.length; i++) {
			try {
				if (null != ignoreColumnStrings[i]
						&& !"".equals(ignoreColumnStrings[i].trim())) {
					this.ignoreColumns.add(i,
							Integer.parseInt(ignoreColumnStrings[i].trim()));
				}
			} catch (NumberFormatException e) {
				logger.error("Could not identify key column for value: '"
						+ ignoreColumnStrings[i]
						+ "'\nList of key columns is incomplete!", e);
				throw e;
			}
		}

		this.modificationTimestamp = 0;
		this.modified = true;
		this.contentMap = new HashMap<String, Content>();

		// this.getContentUrls();
	}

	/**
	 * Optional method if any additional process is required to stop the
	 * provider
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#destroy()
	 */
	@Override
	public void destroy() {
		// Nothing to be done
	}

	/**
	 * Required method to add now or update modified content objects to the
	 * repository
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#doSynchrinization()
	 */
	@Override
	public void doSynchonization() {
		logger.info(String.format("Invoking %s#doSynchronization() ...", this
				.getClass().getSimpleName()));

		Collection<? extends String> contentUrls = getContentUrls();
		if (this.modified) {
			for (String contentUrl : contentUrls) {
				Content content = getContent(contentUrl);
				try {
					if (!isExistingContent(contentUrl)) {
						addContent(content);
					} else {
						updateContent(content);
					}
					// Let the application container take a breath and sleep for
					// 0,1 second.
					Thread.sleep(100);
				} catch (IQserException e) {
					logger.error(
							String.format(
									"Unexpected error while trying to add or update content: %s",
									contentUrl), e);
				} catch (InterruptedException e) {
					logger.warn("Could not sleep well :(");
				}
			}
		}
	}

	/**
	 * Required method to remove content objects from the repository
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#doSynchrinization()
	 */
	@Override
	public void doHousekeeping() {
		logger.info(String.format("Invoking %s#doHousekeeping() ...", this
				.getClass().getSimpleName()));

		Collection<? extends String> contentUrls = getContentUrls();
		for (String existingContentUrl : getExistingContentUrls()) {
			if (!contentUrls.contains(existingContentUrl)) {
				try {
					removeContent(existingContentUrl);
				} catch (IQserException e) {
					logger.error(
							String.format(
									"Unexpected error while trying to delete content: %s",
									existingContentUrl), e);
				}
			}
		}
	}

	/**
	 * Returns the content object for a given content URL.
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#getContent(java.lang.String)
	 */
	@Override
	public Content getContent(String contentUrl) {
		logger.info(String.format("Invoking %s#getContent(String url) ...",
				this.getClass().getSimpleName()));
		if (0 == contentMap.size()) {
			this.getContentUrls();
		}
		return contentMap.get(contentUrl);
	}

	/*
	 * A method to deliver the complete content object, for example to display
	 * the content object
	 * 
	 * @see
	 * com.iqser.core.plugin.ContentProvider#getBinaryData(com.iqser.core.model
	 * .Content)
	 */
	public byte[] getBinaryData(Content c) {
		logger.info(String.format("Invoking %s#getBinaryData(Content c) ...",
				this.getClass().getSimpleName()));

		return c.getFulltext().getBytes();
	}

	/**
	 * This method may be used to do anything with the content objects
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#getActions(com.iqser.core.model.Content)
	 */
	@Override
	public Collection<String> getActions(Content arg0) {
		// This content provider has no actions.
		return null;
	}

	/**
	 * Performs defined actions related a specing content object of this
	 * provider
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#performAction(java.lang.String,
	 *      com.iqser.core.model.Content)
	 */
	@Override
	public void performAction(String arg0, Content arg1) {
		// Nothing to be done, because no actions are defined.
	}

	/**
	 * This method is used to retrieve identifiers for objects in the csv-file.
	 * As it parses the CSV-file which is defined in an init-param of this
	 * plugin to read the contentUrls, the content objects will also be created
	 * "on the fly" and stored in memory. The file will only be parsed if its
	 * modification timestamp has changed.
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#getContentUrls()
	 */
	@Override
	public Collection<String> getContentUrls() {
		logger.info(String.format("Invoking %s#getContentUrls() ...", this
				.getClass().getSimpleName()));

		if (null != this.file && this.file.exists() && this.file.isFile()
				&& this.file.canRead()) {
			logger.info("Reading CSV file: " + this.file.toURI().toString());

			if (this.contentMap.isEmpty()
					|| this.modificationTimestamp < this.file.lastModified()) {
				this.modificationTimestamp = this.file.lastModified();
				this.modified = true;
				this.contentMap.clear();

				CsvReader csvReader = null;
				try {
					csvReader = new CsvReader(new InputStreamReader(
							new FileInputStream(this.file), this.charset),
							delimeter);
				} catch (FileNotFoundException e) {
					logger.error("Could not read file: " + this.file.getPath(),
							e);
					return this.contentMap.keySet();
				}

				int row = 0;
				int columnCount = 0;
				String[] attributes = new String[1];
				try {
					while (csvReader.readRecord()) {
						if (0 < row++) {
							Content content = new Content();
							content.setType(this.getType());
							content.setProvider(this.getId());
							content.setModificationDate(modificationTimestamp);

							if (logger.isDebugEnabled()) {
								logger.debug(String
										.format("Building content object of type '%s'.",
												this.getType()));
							}

							for (int i = 0; i < columnCount; i++) {
								if (null == attributes[i]
										|| "".equals(attributes[i].trim())) {
									// skip this column
									continue;
								}

								String attributeValue = csvReader.get(i).trim();
								if (logger.isDebugEnabled()) {
									logger.debug(String
											.format("\tBuilding attribute object - %s = %s",
													attributes[i],
													attributeValue));
								}

								// removing quotes at the beginning and at
								// the end
								if (attributeValue.startsWith("\"")
										&& attributeValue.endsWith("\"")) {
									attributeValue = attributeValue.substring(
											1, attributeValue.length() - 1)
											.replace("\"\"", "\"");
								}

								Attribute attribute = new Attribute(
										attributes[i], attributeValue,
										Attribute.ATTRIBUTE_TYPE_TEXT,
										this.keyColumns.contains(Integer
												.valueOf(i)));

								if (this.idColumns.contains(Integer.valueOf(i))
										&& null != attribute.getValue()
										&& !"".equals(attribute.getValue()
												.trim())) {
									if (1 == this.idColumns.size()
											&& this.idAsContentUrl) {
										content.setContentUrl(attribute
												.getValue());
									} else if (null == content.getContentUrl()
											|| "".equals(content
													.getContentUrl().trim())) {
										content.setContentUrl(CSV_CONTENT_URI_BASE
												+ URLEncoder.encode(this
														.getType()
														.toLowerCase(),
														"ISO-8859-1")

												+ "/"
												+ URLEncoder.encode(
														attribute.getValue(),
														"ISO-8859-1"));
									} else {
										content.setContentUrl(content
												.getContentUrl()
												.concat("/"
														+ URLEncoder.encode(
																attribute
																		.getValue(),
																"ISO-8859-1")));
									}
								}

								String fulltext = null;
								if (i == this.fulltextColumn) {
									fulltext = attribute.getValue();
								} else if (!this.ignoreColumns.contains(Integer
										.valueOf(i))
										&& null != attribute.getValue()
										&& !"".equals(attribute.getValue()
												.trim())) {
									// empty attributes, the fulltext
									// attribute (if present) and those that
									// should be ignored are not added to
									// the content object
									Attribute existingAttribute = content
											.getAttributeByName(attribute
													.getName());
									if (null != existingAttribute) {
										existingAttribute.addValue(attribute
												.getValue());
									} else {
										content.addAttribute(attribute);
									}
								}

								if (null == fulltext) {
									// take the whole row as fulltext
									fulltext = csvReader.getRawRecord()
											.replace(';', ' ');
								}

								if (null != fulltext) {
									fulltext = fulltext.replace('(', ' ');
									fulltext = fulltext.replace(')', ' ');
									fulltext = fulltext.replace('{', ' ');
									fulltext = fulltext.replace('}', ' ');
									fulltext = fulltext.replace('[', ' ');
									fulltext = fulltext.replace(']', ' ');
									fulltext = fulltext.replace('<', ' ');
									fulltext = fulltext.replace('>', ' ');
									content.setFulltext(fulltext);
								}

							}

							contentMap.put(content.getContentUrl(), content);
						} else {
							// processing first row
							columnCount = csvReader.getColumnCount();
							attributes = new String[columnCount];
							for (int i = 0; i < columnCount; i++) {
								String attribute = csvReader.get(i);
								if (null != attribute
										&& !"".equals(attribute.trim())) {
									attribute = attribute.trim();

									// removing quotes at the beginning and
									// at the end
									if (attribute.trim().startsWith("\"")
											&& attribute.trim().endsWith("\"")) {
										attribute = attribute.substring(1,
												attribute.length() - 1)
												.replace("\"\"", "\"");
									}

									attributes[i] = attribute.toUpperCase()
											.replace("\u00C4", "AE")
											.replace("\u00D6", "OE")
											.replace("\u00DC", "UE")
											.replace("\u00DF", "SS")
											.replace("\"", " ").trim()
											.replace("  ", " ")
											.replace(' ', '_');

									// support former configuration syntax
									if (null == this.settings
											.getProperty(CSV_PROPERTY_IDCOLUMNS)
											&& null == this.settings
													.getProperty(CSV_PROPERTY_IDCOLUMN)) {
										String idColumnProperty = this.settings
												.getProperty(
														CSV_PROPERTY_IDCOLUMN_OBSOLETE,
														"").trim();
										if (!"".equals(idColumnProperty)
												&& attribute
														.equals(idColumnProperty)) {
											this.idColumns.clear();
											this.idColumns.add(Integer
													.valueOf(i));
											this.idAsContentUrl = true;
										}
									}

									// support former configuration syntax
									if (null == this.settings
											.getProperty(CSV_PROPERTY_KEYCOLUMNS)) {
										String keyColumnsProperty = this.settings
												.getProperty(
														CSV_PROPERTY_KEYCOLUMNS_OBSOLETE,
														"").trim();
										if (!"".equals(keyColumnsProperty)
												&& keyColumnsProperty
														.contains("["
																+ attribute
																+ "]")) {
											if (keyColumnCompatibilityMode) {
												keyColumns.add(Integer
														.valueOf(i));
											} else {
												keyColumns.clear();
												keyColumns.add(Integer
														.valueOf(i));
												keyColumnCompatibilityMode = true;
											}
										}
									}
								} // end if
							} // end for
						} // end else
					} // end while
				} catch (IOException e) {
					logger.error("Error occured while reading file: "
							+ CSV_PROPERTY_FILE, e);
				} finally {
					csvReader.close();
				}
			} else {
				this.modified = false;
			}
		} // end if

		return this.contentMap.keySet();

	}

	/**
	 * Deprecated method to use other plugins if a content object contains sub
	 * content .
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#getContent(java.io.InputStream)
	 */
	@Override
	@Deprecated
	public Content getContent(InputStream arg0) {
		throw new NotImplementedException();
	}

	/**
	 * Deprecated methods to use in case of using events of other plugins for
	 * exmplae an email containing a csv file
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#onChangeEvent(com.iqser.core.event.Event)
	 */
	@Override
	@Deprecated
	public void onChangeEvent(Event arg0) {
		// Nothing to be done
	}

}
