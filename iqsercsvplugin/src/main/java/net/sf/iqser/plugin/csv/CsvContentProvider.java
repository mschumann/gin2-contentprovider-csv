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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.queryParser.QueryParser;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import com.iqser.core.exception.IQserException;
import com.iqser.core.model.Attribute;
import com.iqser.core.model.Content;
import com.iqser.core.model.Parameter;
import com.iqser.core.model.Result;
import com.iqser.core.plugin.provider.AbstractContentProvider;

/**
 * Content provider demonstrating how to access CSV files. It should be part of the file plugin package for a productive
 * system.
 * 
 * @author Joerg Wurzer
 * @author Kresnadi Budisantoso
 * 
 */
public class CsvContentProvider extends AbstractContentProvider {

	/** The logger. */
	private static Logger LOG = Logger.getLogger(CsvContentProvider.class);

	private static final String CSV_PROPERTY_FILE = "file";
	private static final String CSV_PROPERTY_DELIMETER = "delimeter";
	private static final String CSV_PROPERTY_CHARSET = "charset";
	private static final String CSV_PROPERTY_IDCOLUMN = "column.id";
	private static final String CSV_PROPERTY_IDASCONTENTURL = "column.idAsContentUrl";
	private static final String CSV_PROPERTY_IDCOLUMNS = "columns.id";
	private static final String CSV_PROPERTY_NAMECOLUMN = "column.name";
	private static final String CSV_PROPERTY_KEYCOLUMNS = "columns.key";
	private static final String CSV_PROPERTY_IGNORECOLUMNS = "columns.ignore";
	private static final String CSV_PROPERTY_TIMESTAMPCOLUMNS = "columns.type.timestamp";
	private static final String CSV_PROPERTY_MODIFICATIONDATECOLUMN = "column.modificationDate";
	private static final String CSV_PROPERTY_FULLTEXTCOLUMN = "column.fulltext";
	private static final String CSV_PROPERTY_CONTENT_TYPE = "content.type";
	private static final String CSV_PROPERTY_RECORDASFULLTEXT = "recordAsFulltext";

	public static final String CSV_DEFAULT_DELIMETER = ";";
	public static final String CSV_DEFAULT_CHARSET = "UTF-8";
	public static final String CSV_DEFAULT_IDCOLUMN = "0";
	public static final String CSV_DEFAULT_IDASCONTENTURL = "false";
	public static final String CSV_DEFAULT_KEYCOLUMNS = "";
	public static final String CSV_DEFAULT_NAMECOLUMN = "-1";
	public static final String CSV_DEFAULT_IGNORECOLUMNS = "";
	public static final String CSV_DEFAULT_TIMESTAMPCOLUMNS = "";
	public static final String CSV_DEFAULT_MODIFICATIONDATECOLUMN = "-1";
	public static final String CSV_DEFAULT_FULLTEXTCOLUMN = "-1";
	public static final String CSV_DEFAULT_TYPE = "CSV Data";
	public static final String CSV_DEFAULT_RECORDASFULLTEXT = "false";

	public static final String CSV_CONTENT_URI_BASE = "iqser://iqsercsvplugin.sf.net";

	private static final Map<String, Long> MODIFICATION_TIMESTAMP_CACHE = new HashMap<String, Long>();

	/* The CSV file. */
	private File file;

	/* The delimeter character of the csv file. */
	private char delimeter;

	/* Zero-based list of columns that will be used as composite id. */
	private List<Integer> idColumns;

	/*
	 * If TRUE, the value of the ID column will be used as ContentUrl, else an artificial URI will be generated.
	 */
	private boolean idAsContentUrl;

	/* The zero-based number of the column that holds the fulltext part. */
	private int fulltextColumn;

	/* The zero-based number of the column that should be used as name column. */
	private int nameColumn;

	/* Zero-based list of columns that will be used as keys. */
	private List<Integer> keyColumns;

	/* Zero-based list of columns that will be ignored. */
	private List<Integer> ignoreColumns;

	/* A boolean flag which indicates whether the file has been modified or not. */
	private boolean modified;

	/* A map of content objects with the content's ContentUrl as keys. */
	private Map<String, Content> contentMap;

	/* The charset of the csv file. */
	private Charset charset;

	private String contentType;

	/* Zero-based list of columns that contains timestamps. */
	private List<Integer> timestampColumns;

	/* Zero-based list of columns that contains timestamps. */
	private int modificationDateColumn = -1;

	private boolean recordAsFulltext = false;

	protected Map<String, Content> getContentMap() {
		return contentMap;
	}

	protected void setContentMap(Map<String, Content> contentMap) {
		this.contentMap = contentMap;
	}

	/*
	 * Returns a set of existing ContentUrls.
	 */
	private Set<String> getExistingContentUrls() throws IQserException {
		Set<String> existingContentUrls = new HashSet<String>();

		for (Content content : getExistingContents()) {
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
		LOG.info(String.format("Invoking %s#init() ...", this.getClass().getSimpleName()));

		// Setting the file's name.
		String filename = getInitParams().getProperty(CSV_PROPERTY_FILE, "").trim();
		if ("".equals(filename)) {
			LOG.error("No csv file specified in configuration!");
			filename = null;
		}

		// Setting the file
		if (null != filename && !"".equals(filename.trim())) {

			URI fileUri = null;
			try {
				fileUri = new URI(filename);
				LOG.info("CSV filename: " + filename);
				LOG.info("URI of CSV file: " + fileUri + ". URI is absolute? " + fileUri.isAbsolute());
			} catch (URISyntaxException e) {
				if (LOG.isDebugEnabled()) {
					LOG.warn(filename + " is no vaild URI.", e);
				} else {
					LOG.warn(filename + " is no vaild URI.");
				}
			}

			if (null == fileUri || !fileUri.isAbsolute()) {
				file = new File(filename);
			} else {
				file = new File(fileUri.getPath());
			}
		}

		// Setting the content type
		contentType = getInitParams().getProperty(CSV_PROPERTY_CONTENT_TYPE, CSV_DEFAULT_TYPE).trim();
		LOG.debug("Init param: contentType = " + contentType);

		// Setting the file's delimeter.
		try {
			delimeter = getInitParams().getProperty(CSV_PROPERTY_DELIMETER, CSV_DEFAULT_DELIMETER).trim().charAt(0);
		} catch (IndexOutOfBoundsException ioobe) {
			LOG.warn(String.format("'%s' is an illegal delimeter. Default delimeter (%s) will be used.", delimeter,
					CSV_DEFAULT_DELIMETER));
			delimeter = CSV_DEFAULT_DELIMETER.charAt(0);
		}
		LOG.debug("Init param: delimeter = " + delimeter);

		// Setting the file's charset.
		String charsetParamValue = getInitParams().getProperty(CSV_PROPERTY_CHARSET, CSV_DEFAULT_CHARSET);
		try {
			charset = Charset.forName(charsetParamValue);
			LOG.debug("Init param: charset = " + charsetParamValue);
		} catch (IllegalCharsetNameException e) {
			charset = Charset.defaultCharset();
			LOG.warn(String.format("'%s' is an illegal chaset name. Default charset (%s) of the JVM will be used.",
					charsetParamValue, charset.displayName()));
		}

		// Setting the zero-based list of numbers of the id-columns.
		String idColumnsParamValue = getInitParams().getProperty(CSV_PROPERTY_IDCOLUMNS);
		if (StringUtils.isBlank(idColumnsParamValue)) {
			idColumnsParamValue = getInitParams().getProperty(CSV_PROPERTY_IDCOLUMN, CSV_DEFAULT_IDCOLUMN);
		}
		if (StringUtils.isBlank(idColumnsParamValue)) {
			idColumnsParamValue = CSV_DEFAULT_IDCOLUMN;
		}
		String[] idColumnStrings = idColumnsParamValue.split(",");
		idColumns = new ArrayList<Integer>();
		for (int i = 0; i < idColumnStrings.length; i++) {
			try {
				if (StringUtils.isNotBlank(idColumnStrings[i])) {
					idColumns.add(i, Integer.parseInt(idColumnStrings[i].trim()));
				}
			} catch (NumberFormatException e) {
				LOG.error("Could not identify id column for value: '" + idColumnStrings[i]
						+ "'\nList of id columns is corrupted!", e);
				throw e;
			}
		}
		LOG.debug("Init param: idColumns = " + idColumnsParamValue);

		// Setting the boolean idAsContentUrl-flag.
		idAsContentUrl = Boolean.parseBoolean(getInitParams().getProperty(CSV_PROPERTY_IDASCONTENTURL,
				CSV_DEFAULT_IDASCONTENTURL));
		LOG.debug("Init param: idAsContentUrl = " + idAsContentUrl);

		// Setting the boolean recordAsFulltext-flag.
		recordAsFulltext = Boolean.parseBoolean(getInitParams().getProperty(CSV_PROPERTY_RECORDASFULLTEXT,
				CSV_DEFAULT_RECORDASFULLTEXT));
		LOG.debug("Init param: recordAsFulltext = " + idAsContentUrl);

		// Setting the zero-based number of the fulltext-column.
		String fulltextColumnValue = getInitParams().getProperty(CSV_PROPERTY_FULLTEXTCOLUMN,
				CSV_DEFAULT_FULLTEXTCOLUMN);
		fulltextColumn = Integer.parseInt(StringUtils.isNotBlank(fulltextColumnValue) ? fulltextColumnValue.trim()
				: CSV_DEFAULT_FULLTEXTCOLUMN);
		LOG.debug("Init param: fulltextColumn = " + fulltextColumn);

		// Setting the zero-based number of the name-column.
		String nameColumnValue = getInitParams().getProperty(CSV_PROPERTY_NAMECOLUMN, CSV_DEFAULT_NAMECOLUMN);
		nameColumn = Integer.parseInt(StringUtils.isNotBlank(nameColumnValue) ? nameColumnValue.trim()
				: CSV_DEFAULT_NAMECOLUMN);
		LOG.debug("Init param: nameColumn = " + nameColumn);

		// Setting the timestamp columns' type.
		String timestampColumnParamValue = getInitParams().getProperty(CSV_PROPERTY_TIMESTAMPCOLUMNS,
				CSV_DEFAULT_TIMESTAMPCOLUMNS);
		if (StringUtils.isBlank(timestampColumnParamValue)) {
			timestampColumnParamValue = CSV_DEFAULT_TIMESTAMPCOLUMNS;
		}
		String[] timestampColumnStrings = timestampColumnParamValue.split(",");
		timestampColumns = new ArrayList<Integer>();
		for (int i = 0; i < timestampColumnStrings.length; i++) {
			try {
				if (!(null == timestampColumnStrings[i] || "".equals(timestampColumnStrings[i].trim()))) {
					timestampColumns.add(i, Integer.parseInt(timestampColumnStrings[i].trim()));
				}
			} catch (NumberFormatException e) {
				LOG.error("Could not identify key column for value: '" + timestampColumnStrings[i]
						+ "'\nList of key columns is corrupted!", e);
				throw e;
			}
		}
		LOG.debug("Init param: columns.type.date = " + timestampColumnStrings);

		// Setting the key columns.
		String keyColumnParamValue = getInitParams().getProperty(CSV_PROPERTY_KEYCOLUMNS, CSV_DEFAULT_KEYCOLUMNS);
		if (StringUtils.isBlank(keyColumnParamValue)) {
			keyColumnParamValue = CSV_DEFAULT_KEYCOLUMNS;
		}
		String[] keyColumnStrings = keyColumnParamValue.split(",");
		keyColumns = new ArrayList<Integer>();
		for (int i = 0; i < keyColumnStrings.length; i++) {
			try {
				if (StringUtils.isNotBlank(keyColumnStrings[i])) {
					keyColumns.add(i, Integer.parseInt(keyColumnStrings[i].trim()));
				}
			} catch (NumberFormatException e) {
				LOG.error("Could not identify key column for value: '" + keyColumnStrings[i]
						+ "'\nList of key columns is corrupted!", e);
				throw e;
			}
		}
		LOG.debug("Init param: keyColumns = " + keyColumnParamValue);

		// Setting the columns that will be ignored while creating the content
		// objects.
		String ignoreColumnParamValue = getInitParams().getProperty(CSV_PROPERTY_IGNORECOLUMNS,
				CSV_DEFAULT_IGNORECOLUMNS);
		if (StringUtils.isBlank(ignoreColumnParamValue)) {
			ignoreColumnParamValue = CSV_DEFAULT_IGNORECOLUMNS;
		}
		String[] ignoreColumnStrings = ignoreColumnParamValue.split(",");
		ignoreColumns = new ArrayList<Integer>();
		for (int i = 0; i < ignoreColumnStrings.length; i++) {
			try {
				if (StringUtils.isNotBlank(ignoreColumnStrings[i])) {
					ignoreColumns.add(i, Integer.parseInt(ignoreColumnStrings[i].trim()));
				}
			} catch (NumberFormatException e) {
				LOG.error("Could not identify key column for value: '" + ignoreColumnStrings[i]
						+ "'\nList of key columns is incomplete!", e);
				throw e;
			}
		}
		LOG.debug("Init param: ignoreColumnStrings = " + ignoreColumnStrings);

		// Setting the zero-based number of the modificationDate-column.
		String modificationDateColumnValue = getInitParams().getProperty(CSV_PROPERTY_MODIFICATIONDATECOLUMN,
				CSV_DEFAULT_MODIFICATIONDATECOLUMN);
		modificationDateColumn = Integer
				.parseInt(StringUtils.isNotBlank(modificationDateColumnValue) ? modificationDateColumnValue.trim()
						: CSV_DEFAULT_NAMECOLUMN);
		LOG.debug("Init param: modificationDateColumn = " + modificationDateColumn);

		modified = true;
		contentMap = new HashMap<String, Content>();

	}

	private long getCachedFileModificationTimestamp() {
		Long cachedTimestamp = MODIFICATION_TIMESTAMP_CACHE.get(getName());
		if (null != cachedTimestamp) {
			return cachedTimestamp.longValue();
		}
		return 0L;
	}

	private void setCachedFileModificationTimestamp(long timestamp) {
		if (timestamp >= 0) {
			MODIFICATION_TIMESTAMP_CACHE.put(getName(), timestamp);
		} else {
			MODIFICATION_TIMESTAMP_CACHE.put(getName(), 0L);
		}
	}

	/**
	 * Optional method if any additional process is required to stop the provider
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#destroy()
	 */
	@Override
	public void destroy() {
		// Nothing to be done
	}

	/**
	 * Required method to add now or update modified content objects to the repository
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#doSynchrinization()
	 */
	@Override
	public void doSynchronization() {
		LOG.info(String.format("Invoking %s#doSynchronization() ...", this.getClass().getSimpleName()));

		if (0 == contentMap.size()) {
			getContentUrls();
		}

		if (modified) {
			for (String contentUrl : contentMap.keySet()) {
				Content content = createContent(contentUrl);
				try {
					if (isExistingContent(contentUrl)) {
						if (isModifiedContent(content)) {
							LOG.info(String.format("Invoking %s#updateContent() for ContentURL: %s ...", this
									.getClass().getSimpleName(), contentUrl));
							updateContent(content);
						} else {
							LOG.info(String.format("Skipping unmodified content: %s", contentUrl));
						}
					} else {
						LOG.info(String.format("Invoking %s#addContent() for ContentURL: %s ...", this.getClass()
								.getSimpleName(), contentUrl));
						addContent(content);
					}
				} catch (IQserException e) {
					LOG.error(String.format("Unexpected error while trying to add or update content: %s", contentUrl),
							e);
				}
			}
		}
	}

	private boolean isModifiedContent(Content content) throws IQserException {

		if (0 > modificationDateColumn) {
			// modification all content objects is equal -> content comparison is necessary
			return !equalIgnoringModificationDate(content, getExistingContent(content.getContentUrl()));
		} else {
			String query = String.format("+%s:\"%s\" +%s:\"%s\"", Content.CONTENT_FIELD_PROVIDER,
					QueryParser.escape(getName()), Content.CONTENT_FIELD_URL,
					QueryParser.escape(content.getContentUrl()));
			if (LOG.isDebugEnabled()) {
				LOG.debug("isModifiedContent: query = " + query);
			}

			Collection<Result> results = search(query);
			if (null != results && 0 < results.size()) {
				Result result = results.iterator().next();
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("isModifiedContent: comparing modification dates (%s, %s)",
							result.getModificationDate(), content.getModificationDate()));
				}
				return content.getModificationDate() > result.getModificationDate();
			}
			return false;
		}

	}

	/**
	 * Checks if the new content and the old content are different.
	 * 
	 * @param Content
	 *            contentNew
	 * @param Content
	 *            contentOld
	 * @return boolean
	 */
	protected static boolean equalIgnoringModificationDate(Content content, Content other) {
		if (content == other) {
			// objects are identical or both null
			return true;
		} else {
			if (content == null || other == null) {
				return false;
			}

			if (other.getContentUrl() == null && content.getContentUrl() != null) {
				return false;
			}
			if (other.getContentUrl() != null && !other.getContentUrl().equals(content.getContentUrl())) {
				return false;
			}

			if (other.getProvider() == null && content.getProvider() != null) {
				return false;
			}
			if (other.getProvider() != null && !other.getProvider().equals(content.getProvider())) {
				return false;
			}

			if (other.getType() == null && content.getType() != null) {
				return false;
			}
			if (other.getType() != null && !other.getType().equals(content.getType())) {
				return false;
			}

			if (other.getFulltext() == null && content.getFulltext() != null) {
				return false;
			}
			if (other.getFulltext() != null && !other.getFulltext().equals(content.getFulltext())) {
				return false;
			}

			// Check the contents attributes
			if (other.getAttributes() == null && content.getAttributes() != null) {
				return false;
			}
			if (other.getAttributes() != null && content.getAttributes() == null) {
				return false;
			}
			if (other.getAttributes().size() != content.getAttributes().size()) {
				return false;
			}

			Iterator<Attribute> iteratorContent = content.getAttributes().iterator();
			while (iteratorContent.hasNext()) {
				Attribute attr = iteratorContent.next();

				boolean hasFound = false;

				Iterator<Attribute> iteratorContentB = other.getAttributes().iterator();
				while (iteratorContentB.hasNext()) {
					Attribute attrB = iteratorContentB.next();
					if (attr.equals(attrB)) {
						hasFound = true;
						break;
					}
				}

				if (!hasFound) {
					return false;
				}
			}

			return true;
		}
	}

	/**
	 * Required method to remove content objects from the repository
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#doSynchrinization()
	 */
	@Override
	public void doHousekeeping() {
		LOG.info(String.format("Invoking %s#doHousekeeping() ...", this.getClass().getSimpleName()));

		Collection<? extends String> contentUrls = getContentUrls();
		try {
			for (String existingContentUrl : getExistingContentUrls()) {
				if (!contentUrls.contains(existingContentUrl)) {
					try {
						removeContent(existingContentUrl);
					} catch (IQserException e) {
						LOG.error(String.format("Unexpected error while trying to delete content: %s",
								existingContentUrl), e);
					}
				}
			}
		} catch (IQserException e) {
			LOG.error("Unexpected error while trying to get existing content URLs.", e);
		}
	}

	/**
	 * Returns the content object for a given content URL.
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#getContent(java.lang.String)
	 */
	@Override
	public Content createContent(String contentUrl) {
		LOG.info(String.format("Invoking %s#createContent(%s) ...", this.getClass().getSimpleName(), contentUrl));

		if (0 == contentMap.size()) {
			getContentUrls();
		}
		return contentMap.get(contentUrl);
	}

	/*
	 * A method to deliver the complete content object, for example to display the content object
	 * 
	 * @see com.iqser.core.plugin.ContentProvider#getBinaryData(com.iqser.core.model .Content)
	 */
	@Override
	public byte[] getBinaryData(Content c) {
		LOG.info(String.format("Invoking %s#getBinaryData(Content c) ...", this.getClass().getSimpleName()));
		if (null != c.getFulltext()) {
			return c.getFulltext().getBytes();
		}
		return "".getBytes();
	}

	/**
	 * This method may be used to do anything with the content objects
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#getActions(com.iqser.core.model.Content)
	 */
	@Override
	public Collection<String> getActions(Content arg0) {
		return Action.getAllActions();
	}

	/**
	 * This method is used to retrieve identifiers for objects in the csv-file. As it parses the CSV-file which is
	 * defined in an init-param of this plugin to read the contentUrls, the content objects will also be created
	 * "on the fly" and stored in memory. The file will only be parsed if its modification timestamp has changed.
	 * 
	 * @see com.iqser.core.plugin.AbstractContentProvider#getContentUrls()
	 */
	public Collection<String> getContentUrls() {
		LOG.info(String.format("Invoking %s#getContentUrls() ...", this.getClass().getSimpleName()));

		if (null != file && file.exists() && file.isFile() && file.canRead()) {
			LOG.info("Reading CSV file: " + file.toURI().toString());

			if (contentMap.isEmpty() || getCachedFileModificationTimestamp() < file.lastModified()) {
				setCachedFileModificationTimestamp(file.lastModified());
				modified = true;
				contentMap.clear();

				CsvReader csvReader = null;
				try {
					csvReader = new CsvReader(new FileInputStream(file), delimeter, charset);
				} catch (FileNotFoundException e) {
					LOG.error("Could not read file: " + file.getPath(), e);
					return contentMap.keySet();
				}

				try {
					csvReader.readHeaders();
					List<Column> columns = getColumns(csvReader);

					while (csvReader.readRecord()) {
						Content content = getContentFromCurrentRecord(columns, csvReader);
						if (StringUtils.isNotBlank(content.getContentUrl())) {
							contentMap.put(content.getContentUrl(), content);
						}
					}

				} catch (IOException e) {
					LOG.error("Error occured while reading file: " + file.getPath(), e);
				} finally {
					csvReader.close();
				}
			} else {
				modified = false;
			}
		} else {
			LOG.error("Cannot read CSV file: " + file.toURI().toString());
		} // end if else

		return contentMap.keySet();

	}

	protected Content getContentFromCurrentRecord(List<Column> columns, CsvReader csvReader) throws IOException,
			UnsupportedEncodingException {
		Content content = new Content();
		content.setType(contentType);
		content.setProvider(getName());

		if (0 > modificationDateColumn) {
			content.setModificationDate(getCachedFileModificationTimestamp());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Building content object of type '%s'.", contentType));
		}

		String fulltext = null;
		for (Column column : columns) {
			Attribute attribute = getAttributeFromCurrentColumnOfCurrentRecord(column, csvReader);

			if (null != attribute) {
				String attributeValue = attribute.getValue();

				// set contentUrl
				if (column.isIdColumn()) {
					content.setContentUrl(createContentUrl(content.getContentUrl(), column.getName(), attributeValue));
				}

				// set modificationDate
				if (column.isModifiedDateColumn()) {
					content.setModificationDate(extractModifactionDate(attributeValue));
				}

				// TODO: replace with mapping of column to attribute names
				if (column.isNameColumn() && null == content.getAttributeByName("NAME")) {
					content.addAttribute(new Attribute("NAME", attributeValue, Attribute.ATTRIBUTE_TYPE_TEXT, false));
				}

				if (column.isFulltextColumn()) {
					fulltext = attributeValue;
				} else {
					// empty attributes, the fulltext attribute (if present) and those that should
					// be ignored are not added to the content object
					Attribute existingAttribute = content.getAttributeByName(attribute.getName());
					if (null != existingAttribute) {
						existingAttribute.addValue(attributeValue);
					} else {
						content.addAttribute(attribute);
					}
				}

			} // end if attribute value is not blank
		} // end for

		// set fulltext
		if (null == fulltext && recordAsFulltext) {
			content.setFulltext(csvReader.getRawRecord().replace(delimeter, ' '));
		} else {
			content.setFulltext(fulltext);
		}

		return content;
	}

	protected long extractModifactionDate(String attributeValue) {
		long modificationDate = -1;

		try {
			modificationDate = Long.parseLong(attributeValue);
		} catch (NumberFormatException e) {
			LOG.error("Bad value for modification date: " + attributeValue, e);
		}

		if (0 > modificationDate) {
			modificationDate = getCachedFileModificationTimestamp();
		}
		return modificationDate;
	}

	protected String createContentUrl(String existingContentUrl, String columnName, String attributeValue)
			throws UnsupportedEncodingException {
		String contentUrl;
		if (1 == idColumns.size() && idAsContentUrl) {
			contentUrl = attributeValue;
		} else {
			if (StringUtils.isEmpty(existingContentUrl)) {
				contentUrl = String.format("%s/%s?%s=%s", CSV_CONTENT_URI_BASE,
						URLEncoder.encode(contentType.toLowerCase(), "ISO-8859-1"),
						URLEncoder.encode(columnName, "ISO-8859-1"), URLEncoder.encode(attributeValue, "ISO-8859-1"));
			} else {
				contentUrl = String.format("%s&%s=%s", existingContentUrl, URLEncoder.encode(columnName, "ISO-8859-1"),
						URLEncoder.encode(attributeValue, "ISO-8859-1"));
			}
		}
		return contentUrl;
	}

	protected Attribute getAttributeFromCurrentColumnOfCurrentRecord(Column column, CsvReader csvReader)
			throws IOException {
		String attributeValue = getAttributeValueFromCurrentColumnOfCurrentRecord(column, csvReader);

		Attribute attribute = null;
		if (StringUtils.isNotBlank(attributeValue)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("\tBuilding attribute object: %s --> %s = %s", column.getName(),
						column.getAttributeName(), attributeValue));
			}

			attribute = new Attribute();
			attribute.setName(column.getAttributeName());
			attribute.addValue(attributeValue);

			if (column.isTimestampColumn()) {
				attribute.setType(Attribute.ATTRIBUTE_TYPE_DATE);
			} else {
				attribute.setType(Attribute.ATTRIBUTE_TYPE_TEXT);
			}

			attribute.setKey(column.isKeyColumn());
		}
		return attribute;
	}

	protected String getAttributeValueFromCurrentColumnOfCurrentRecord(Column column, CsvReader csvReader)
			throws IOException {
		String attributeValue = csvReader.get(column.getIndex()).trim();
		// removing quotes at the beginning and at the end
		if (attributeValue.startsWith("\"") && attributeValue.endsWith("\"")) {
			attributeValue = attributeValue.substring(1, attributeValue.length() - 1).replace("\"\"", "\"");
		}
		return attributeValue;
	}

	protected List<Column> getColumns(CsvReader csvReader) throws IOException {
		List<Column> columns = new ArrayList<Column>();

		// processing first row
		int headerCount = csvReader.getHeaderCount();
		for (int i = 0; i < headerCount; i++) {

			String columnName = csvReader.getHeader(i);
			if (!ignoreColumns.contains(Integer.valueOf(i)) && StringUtils.isNotBlank(columnName)) {
				Column column = new Column(i, columnName);

				column.setKeyColumn(keyColumns.contains(Integer.valueOf(i)));
				column.setTimestampColumn(timestampColumns.contains(Integer.valueOf(i)));
				column.setIdColumn(idColumns.contains(Integer.valueOf(i)));

				column.setNameColumn(i == nameColumn);
				column.setFulltextColumn(i == fulltextColumn);
				column.setModifiedDateColumn(i == modificationDateColumn);

				columns.add(column);
			}
		}

		return columns;
	}

	@Override
	public void performAction(String actionName, Collection<Parameter> parameters, Content content) {
		try {
			ActionRunner actionRunner = Action.valueOf(actionName).actionRunner;
			if (null != actionRunner) {
				actionRunner.run(parameters, content, this);
			} else {
				LOG.error("ActionRunner is null for action: " + actionName);
			}
		} catch (IllegalArgumentException e) {
			LOG.error("Cannot perform action: " + actionName, e);
		} catch (NullPointerException e) {
			LOG.error("Cannot perform action: " + null, e);
		}
	}

	@Override
	public Content createContent(InputStream arg0) {
		return null;
	}

	public static enum Action {
		UPDATE(new UpdateActionRunner());

		private final ActionRunner actionRunner;

		private Action(ActionRunner actionRunner) {
			this.actionRunner = actionRunner;
		}

		public static Collection<String> getAllActions() {
			return Arrays.asList(UPDATE.name());
		}
	}

	protected static void clearCache() {
		MODIFICATION_TIMESTAMP_CACHE.clear();
	}

	public synchronized void updateCsv(Content content) {
		if (null == content) {
			LOG.error("Cannot update content: null");
		}

		// update content in CSV file
		// strategy: read original file and write to temporary file synchronized

		// 1. open reader
		CsvReader csvReader = null;
		try {
			csvReader = new CsvReader(new FileInputStream(file), delimeter, charset);
		} catch (FileNotFoundException e) {
			LOG.error("Could not read file: " + file.getPath(), e);
			return;
		}

		// 2. open writer
		File tempFile = null;
		try {
			tempFile = File.createTempFile(file.getName() + "-", "");
		} catch (IOException e) {
			LOG.error("Could not create temporary file!", e);
		}

		if (null != tempFile) {
			CsvWriter csvWriter = null;
			try {
				// use FileWriter constructor that specifies open for appending
				csvWriter = new CsvWriter(new FileOutputStream(tempFile, false), delimeter, charset);

				// read and write the header line
				csvReader.readHeaders();
				for (String header : csvReader.getHeaders()) {
					csvWriter.write(header);
				}
				csvWriter.endRecord();

				List<Column> columns = getColumns(csvReader);

				while (csvReader.readRecord()) {
					String[] values = csvReader.getValues();

					String contentUrl = null;
					for (Column column : columns) {
						if (column.isIdColumn()) {
							String attributeValue = getAttributeValueFromCurrentColumnOfCurrentRecord(column, csvReader);
							if (StringUtils.isNotBlank(attributeValue)) {
								contentUrl = createContentUrl(contentUrl, column.getName(), attributeValue);
							}
						}
					}

					if (null != contentUrl && contentUrl.equals(content.getContentUrl())) {
						// update record values
						if (0 <= fulltextColumn) {
							values[fulltextColumn] = content.getFulltext();
						}

						for (Attribute attribute : content.getAttributes()) {
							for (Column column : columns) {
								if (column.getAttributeName().equals(attribute.getName())) {
									values[column.getIndex()] = attribute.getValue();
									break;
								}
							}
						}
					}

					csvWriter.writeRecord(values);
				}

			} catch (IOException e) {
				LOG.error(
						String.format("Error occured while either reading file '%s' or writing file '%s'.",
								file.getPath(), tempFile.getPath()), e);
			} finally {
				if (null != csvWriter) {
					csvWriter.close();
				}
				if (null != csvReader) {
					csvReader.close();
				}
			}
		}

		if (file.canWrite()) {
			if (file.delete()) {
				if (tempFile.renameTo(file)) {
					LOG.info("CSV record updated successfully: " + content.getContentUrl());

					// update content in content map
					contentMap.put(content.getContentUrl(), content);

					// update content in repository
					try {
						updateContent(content);
						if (LOG.isDebugEnabled()) {
							LOG.debug("Synchronized content: " + content.getContentUrl());
						}
					} catch (IQserException e) {
						LOG.error(
								String.format("Unexpected error while trying to update content: %s",
										content.getContentUrl()), e);
					}
				} else {
					LOG.error("Could not update CSV record for content: " + content.getContentUrl());
				}
			} else {
				LOG.error("CSV file is not deleteable: " + file.getAbsolutePath());
			}
		} else {
			LOG.error("CSV file is not writeable: " + file.getAbsolutePath());
		}
	}

	public static class Column {
		private final int index;
		private final String name;

		private boolean modifiedDateColumn = false;
		private boolean keyColumn = false;
		private boolean timestampColumn = false;
		private boolean idColumn = false;
		private boolean fulltextColumn = false;
		private boolean nameColumn = false;

		public Column(int index, String name) {
			this.index = index;
			this.name = name;
		}

		public String getAttributeName() {
			String modifiedName = name.toUpperCase().trim();

			modifiedName = modifiedName.replaceAll(Pattern.quote("_"), " ");
			modifiedName = modifiedName.replaceAll(Pattern.quote("Ä"), "AE");
			modifiedName = modifiedName.replaceAll(Pattern.quote("Ü"), "UE");
			modifiedName = modifiedName.replaceAll(Pattern.quote("Ö"), "OE");
			modifiedName = modifiedName.replaceAll(Pattern.quote("ß"), "SS");
			modifiedName = modifiedName.replaceAll("[^0-9A-Z\\.\\-_]+", "_");

			return modifiedName;
		}

		public String getName() {
			return name;
		}

		public int getIndex() {
			return index;
		}

		public boolean isModifiedDateColumn() {
			return modifiedDateColumn;
		}

		public void setModifiedDateColumn(boolean modifiedDateColumn) {
			this.modifiedDateColumn = modifiedDateColumn;
		}

		public boolean isKeyColumn() {
			return keyColumn;
		}

		public void setKeyColumn(boolean keyColumn) {
			this.keyColumn = keyColumn;
		}

		public boolean isTimestampColumn() {
			return timestampColumn;
		}

		public void setTimestampColumn(boolean timestampColumn) {
			this.timestampColumn = timestampColumn;
		}

		public boolean isIdColumn() {
			return idColumn;
		}

		public void setIdColumn(boolean idColumn) {
			this.idColumn = idColumn;
		}

		public boolean isFulltextColumn() {
			return fulltextColumn;
		}

		public void setFulltextColumn(boolean fulltextColumn) {
			this.fulltextColumn = fulltextColumn;
		}

		public boolean isNameColumn() {
			return nameColumn;
		}

		public void setNameColumn(boolean nameColumn) {
			this.nameColumn = nameColumn;
		}

	}
}
