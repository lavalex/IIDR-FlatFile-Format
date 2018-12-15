/* _______________________________________________________ {COPYRIGHT-TOP} _____
 * IBM Confidential
 * IBM InfoSphere Data Replication Source Materials
 *
 * 5725-E30 IBM InfoSphere Data Replication
 * 5725-E30 IBM InfoSphere Data Replication for CDC for Netezza Technology
 * 5725-E30 IBM InfoSphere Data Replication for Database Migration
 *
 * 5724-U70 IBM InfoSphere Change Data Delivery
 * 5724-U70 IBM InfoSphere Change Data Delivery for Netezza Technology
 * 5724-Q36 IBM InfoSphere Change Data Delivery for Information Server
 * 5724-Q36 IBM InfoSphere Change Data Delivery for Netezza Technology for Information Server
 *
 * (C) Copyright IBM Corp. 2001, 2012  All Rights Reserved.
 *
 * The source code for this program is not published or otherwise
 * divested of its trade secrets, irrespective of what has been
 * deposited with the U.S. Copyright Office.
 * ________________________________________________________ {COPYRIGHT-END} _____*/

/****************************************************************************
** 
** The following sample of source code ("Sample") is owned by International 
** Business Machines Corporation or one of its subsidiaries ("IBM") and is 
** copyrighted and licensed, not sold. You may use, copy, modify, and 
** distribute the Sample in any form without payment to IBM.
** 
** The Sample code is provided to you on an "AS IS" basis, without warranty of 
** any kind. IBM HEREBY EXPRESSLY DISCLAIMS ALL WARRANTIES, EITHER EXPRESS OR 
** IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF 
** MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. Some jurisdictions do 
** not allow for the exclusion or limitation of implied warranties, so the above 
** limitations or exclusions may not apply to you. IBM shall not be liable for 
** any damages you suffer as a result of using, copying, modifying or 
** distributing the Sample, even if IBM has been advised of the possibility of 
** such damages.
*****************************************************************************/

package com.ibm.replication.cdc.userexit.flatfile;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import com.datamirror.ts.jrncontrol.JournalControlFieldRegistry;
import com.datamirror.ts.target.publication.UserExitJournalHeader;
import com.datamirror.ts.target.publication.userexit.DataRecordIF;
import com.datamirror.ts.target.publication.userexit.DataTypeConversionException;
import com.datamirror.ts.target.publication.userexit.ReplicationEventIF;
import com.datamirror.ts.target.publication.userexit.UserExitException;
import com.datamirror.ts.target.publication.userexit.datastage.DataStageDataFormatIF;
import com.datamirror.ts.util.trace.Trace;

/**
 * 
 * Format the data suitable for the DataStage sequential file reader and column
 * importer stages.
 *
 *
 */
public class FlatFileDataFormat implements DataStageDataFormatIF {

	public final char SUB_RLA_STANDARD = 'Y';
	public final char SUB_RLA_AUDIT = 'A';
	public final char SUB_RLA_AUDIT_B4 = 'B';
	public final char SUB_RLA_INS_UPD = 'I';
	public final char SUB_RLA_DEL_NONE = 'D';
	public final char SUB_RLA_NONE = 'N';
	public final char SUB_RLA_NON_UPD = 'U';

	public static final int BYTE_BUFFER_AUTO_INCREMENT_SIZE = 10000;
	public static final int BYTE_BUFFER_AUTO_INCREMENT_BREATHING_SPACE = 1000;
	public static final int BYTE_BUFFER_SPACE_FOR_FIELD_SEPARATORS = 100;
	private static final int NUM_TRAILING_COLUMNS = JournalControlFieldRegistry.getNumberOfJournalControlFields();

	private SimpleDateFormat outTimestampFormat;
	private SimpleDateFormat outDateOnlyFormat = new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat outTimeOnlyFormat = new SimpleDateFormat("HH:mm:ss");

	private int clobTruncationPoint;
	private int blobTruncationPoint;

	private String FIXED_QUOTE;
	private String FIXED_QUOTE_COLON_QUOTE;
	private String FIXED_COMMA;
	private String FIXED_LEFT_CURLY;
	private String FIXED_RIGHT_CURLY;
	private byte[] COMMA_AS_BYTE_ARRAY;
	private byte[] QUOTE_AS_BYTE_ARRAY;
	private byte[] COMMA_QUOTE_AS_BYTE_ARRAY;
	private byte[] QUOTE_COMMA_QUOTE_AS_BYTE_ARRAY;
	private byte[] QUOTE_COMMA_AS_BYTE_ARRAY;
	private byte[] ZERO_AS_BYTE_ARRAY = getAsByteArrayInUtf8("0");
	private byte[] ONE_AS_BYTE_ARRAY = getAsByteArrayInUtf8("1");

	private String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd hh:mm:ss";
	private String DEFAULT_COLUMN_SEPARATOR = ",";
	private String DEFAULT_COLUMN_DELIMITER = "\"";
	private String DEFAULT_NEW_LINE = "\n";
	private String DEFAULT_ESCAPE_CHARACTER = "\\";

	private String lineOutputFormat = "CSV";
	private boolean csvOutput = true;
	private boolean overrideJournalControlTimestampFormat = false;
	private String journalControlTimestampFormat = DEFAULT_DATETIME_FORMAT;
	private boolean overrideTimestampColumnFormat = false;
	private String timestampColumnFormat = DEFAULT_DATETIME_FORMAT;
	private String newLine = DEFAULT_NEW_LINE;
	private String columnSeparator = DEFAULT_COLUMN_SEPARATOR;
	private String columnDelimiter = DEFAULT_COLUMN_DELIMITER;
	private boolean stripControlCharacters = true;
	private boolean escapeControlCharacters = false;
	private String escapeCharacter = DEFAULT_ESCAPE_CHARACTER;
	private boolean stripTrailingSpaces = false;

	private boolean afterImage = false;

	ByteBuffer outBuffer = ByteBuffer.allocate(BYTE_BUFFER_AUTO_INCREMENT_SIZE);
	ByteBuffer csvNullImage = null;
	private int currentOpType;

	public static byte[] getAsByteArrayInUtf8(String inString) {
		byte[] retval;

		try {
			retval = inString.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// This should never happen, but if it did, we will use the default
			// encoding.
			retval = inString.getBytes();
		}
		return retval;
	}

	public static ByteBuffer addUtf8StringToByteBuffer(ByteBuffer buf, String inString)
			throws DataTypeConversionException {
		ByteBuffer retVal;
		byte[] asBytes;

		try {
			asBytes = inString.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new DataTypeConversionException("UTF-8 isn't available in JVM");

		}

		if (buf.capacity() < buf.position() + asBytes.length + BYTE_BUFFER_SPACE_FOR_FIELD_SEPARATORS) {
			int increment = BYTE_BUFFER_AUTO_INCREMENT_SIZE;
			if (increment < asBytes.length) {
				increment = asBytes.length + BYTE_BUFFER_AUTO_INCREMENT_BREATHING_SPACE;
			}
			retVal = ByteBuffer.allocate(buf.capacity() + increment);
			buf.flip();
			retVal.put(buf);
		} else {
			retVal = buf;
		}

		retVal.put(asBytes);

		return retVal;
	}

	public static ByteBuffer addByteToByteBuffer(ByteBuffer buf, byte inByte) {
		ByteBuffer retVal;

		if (buf.capacity() < buf.position() + 1 + BYTE_BUFFER_SPACE_FOR_FIELD_SEPARATORS) {
			int increment = 10000;
			retVal = ByteBuffer.allocate(buf.capacity() + increment);
			buf.flip();
			retVal.put(buf);
		} else {
			retVal = buf;
		}

		retVal.put(inByte);

		return retVal;
	}

	public static ByteBuffer addBytesToByteBuffer(ByteBuffer buf, byte[] asBytes) {
		ByteBuffer retVal;

		if (buf.capacity() < buf.position() + asBytes.length + BYTE_BUFFER_SPACE_FOR_FIELD_SEPARATORS) {
			int increment = 10000;
			if (increment < asBytes.length) {
				increment = asBytes.length + 1000;
			}
			retVal = ByteBuffer.allocate(buf.capacity() + increment);
			buf.flip();
			retVal.put(buf);
		} else {
			retVal = buf;
		}

		retVal.put(asBytes);

		return retVal;
	}

	public FlatFileDataFormat() throws UserExitException {

		loadConfigurationProperties();

		FIXED_QUOTE = "\"";
		FIXED_QUOTE_COLON_QUOTE = FIXED_QUOTE + ":" + FIXED_QUOTE;
		FIXED_COMMA = ",";
		FIXED_LEFT_CURLY = "{";
		FIXED_RIGHT_CURLY = "}";
		COMMA_AS_BYTE_ARRAY = getAsByteArrayInUtf8(columnSeparator);
		QUOTE_AS_BYTE_ARRAY = getAsByteArrayInUtf8(columnDelimiter);
		COMMA_QUOTE_AS_BYTE_ARRAY = getAsByteArrayInUtf8(columnSeparator + columnDelimiter);
		QUOTE_COMMA_QUOTE_AS_BYTE_ARRAY = getAsByteArrayInUtf8(columnDelimiter + columnSeparator + columnDelimiter);
		QUOTE_COMMA_AS_BYTE_ARRAY = getAsByteArrayInUtf8(columnDelimiter + columnSeparator);
	}

	/*
	 * Load the configuration from the properties file found in the classpath
	 */
	private void loadConfigurationProperties() throws UserExitException {

		Properties prop = new Properties();
		InputStream configFileStream = null;

		// Load the configuration properties from the
		// FlatFileDataFormat.properties file
		String propertiesFile = this.getClass().getSimpleName() + ".properties";

		try {

			URL fileURL = this.getClass().getClassLoader().getResource(propertiesFile);
			Trace.traceAlways(
					"Loading properties for data formatter " + this.getClass().getName() + " from file " + fileURL);
			configFileStream = this.getClass().getClassLoader().getResourceAsStream(propertiesFile);
			prop.load(configFileStream);
			configFileStream.close();

			lineOutputFormat = getProperty(prop, "lineOutputFormat", "CSV");
			if (lineOutputFormat.equalsIgnoreCase("CSV"))
				csvOutput = true;
			else if (lineOutputFormat.equals("JSON"))
				csvOutput = false;
			else
				throw new UserExitException("Invalid value " + lineOutputFormat + " for property lineOutputFormat");

			overrideJournalControlTimestampFormat = getPropertyBoolean(prop, "overrideJournalControlTimestampFormat",
					false);
			journalControlTimestampFormat = getProperty(prop, "journalControlTimestampFormat", DEFAULT_DATETIME_FORMAT);
			overrideTimestampColumnFormat = getPropertyBoolean(prop, "overrideTimestampColumnFormat", false);
			timestampColumnFormat = getProperty(prop, "timestampColumnFormat", DEFAULT_DATETIME_FORMAT);
			columnSeparator = getProperty(prop, "columnSeparator", DEFAULT_COLUMN_SEPARATOR);
			columnDelimiter = getProperty(prop, "columnDelimiter", DEFAULT_COLUMN_DELIMITER);
			newLine = getProperty(prop, "newLine", DEFAULT_NEW_LINE);
			stripControlCharacters = getPropertyBoolean(prop, "stripControlCharacters", true);
			escapeControlCharacters = getPropertyBoolean(prop, "escapeControlCharacters", false);
			escapeCharacter = getProperty(prop, "escapeCharacter", DEFAULT_NEW_LINE);
			stripTrailingSpaces = getPropertyBoolean(prop, "stripTrailingSpaces", false);

			// Set the default format for timestamps
			outTimestampFormat = new SimpleDateFormat(timestampColumnFormat);

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (configFileStream != null) {
				try {
					configFileStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/*
	 * Get property string value
	 */
	private String getProperty(Properties properties, String property, String defaultValue) {
		String value = defaultValue;
		try {
			value = properties.getProperty(property, defaultValue);
		} catch (Exception e) {
			Trace.traceAlways("Error obtaining property " + property + ", using default value " + value);
		}
		return value;
	}

	/*
	 * Get property boolean value
	 */
	private boolean getPropertyBoolean(Properties properties, String property, boolean defaultValue) {
		boolean value = defaultValue;
		try {
			value = Boolean.parseBoolean(properties.getProperty(property, Boolean.toString(defaultValue)));
		} catch (Exception e) {
			Trace.traceAlways(
					"Error obtaining or converting property " + property + " to boolean, using default value " + value);
		}
		return value;
	}

	/**
	 * Remember the truncation points.
	 */
	public void setLobTruncationPoint(int maxClobLengthInChars, int maxBlobLengthInBytes) {
		clobTruncationPoint = maxClobLengthInChars;
		blobTruncationPoint = maxBlobLengthInBytes;
	}

	/**
	 * Create a string containing the data images for the row.
	 */
	public ByteBuffer formatDataImage(DataRecordIF image) throws DataTypeConversionException {
		boolean needToCloseQuote = false;
		outBuffer.position(0);

		if (image != null) {
			for (int i = 1; i <= image.getColumnCount() - NUM_TRAILING_COLUMNS; i++) {
				// Determine column name (prefix with B_ if before image of
				// update)
				String columnName = image.getColumnName(i);
				if (!afterImage && currentOpType == DataStageDataFormatIF.FULL_UPDATE_RECORD)
					columnName = "B_" + columnName;
				Object colObj = image.getObject(i);

				// For NULL values, we just leave the field empty
				if (colObj != null) {
					if (csvOutput) {
						// For performance, we have this wacky logic to only do
						// one add of stuff between columns
						if (needToCloseQuote) {
							outBuffer.put(QUOTE_COMMA_QUOTE_AS_BYTE_ARRAY);
						} else {
							outBuffer.put(COMMA_QUOTE_AS_BYTE_ARRAY);
						}
						needToCloseQuote = true;
					} else {
						outBuffer = addUtf8StringToByteBuffer(outBuffer, FIXED_COMMA);
					}

					if (colObj instanceof Time) {
						addStringElement(outBuffer, columnName, outTimeOnlyFormat.format((Time) colObj));
					} else if (colObj instanceof Timestamp) {
						String outString = null;
						if (!overrideTimestampColumnFormat)
							outString = ((Timestamp) colObj).toString();
						else
							outString = outTimestampFormat.format((Timestamp) colObj);
						addStringElement(outBuffer, columnName, outString);
					} else if (colObj instanceof Date) // This must be checked
														// after Time,
														// Timestamp, as such
														// objects are also Date
														// objects
					{
						addStringElement(outBuffer, columnName, outDateOnlyFormat.format((Date) colObj));
					} else if (colObj instanceof byte[]) {
						byte[] val = (byte[]) colObj;
						if (val.length > blobTruncationPoint) {
							byte[] truncVal = new byte[blobTruncationPoint];
							ByteBuffer truncBuffer = ByteBuffer.wrap(truncVal);
							truncBuffer.put(val, 0, blobTruncationPoint);
							val = truncVal;
						}
						if (!csvOutput)
							outBuffer = addUtf8StringToByteBuffer(outBuffer,
									FIXED_QUOTE + columnName + FIXED_QUOTE_COLON_QUOTE);
						outBuffer = addBytesToByteBuffer(outBuffer, val);
						if (!csvOutput)
							outBuffer = addUtf8StringToByteBuffer(outBuffer, FIXED_QUOTE);
					} else if (colObj instanceof Boolean) {
						String outString = null;
						if (((Boolean) colObj).booleanValue())
							outString = new String(ONE_AS_BYTE_ARRAY);
						else
							outString = new String(ZERO_AS_BYTE_ARRAY);
						addStringElement(outBuffer, columnName, outString);
					} else if (colObj instanceof String) {
						String val = ((String) colObj);
						if (val.length() > clobTruncationPoint) {
							val = val.substring(0, clobTruncationPoint);
						}

						// Strip trailing spaces from the string
						if (stripTrailingSpaces)
							val = val.replaceAll("\\s+$", "");

						// Strip control characters from the string
						if (stripControlCharacters) {
							if (!columnSeparator.isEmpty())
								val = val.replace(columnSeparator, "");
							if (!columnDelimiter.isEmpty())
								val = val.replace(columnDelimiter, "");
							if (!newLine.isEmpty())
								val = val.replace(newLine, "");
						} else {
							// Escape control characters in the string
							if (escapeControlCharacters) {
								val = val.replace(escapeCharacter, escapeCharacter + escapeCharacter);
								if (!columnSeparator.isEmpty())
									val = val.replace(columnSeparator, escapeCharacter + columnSeparator);
								if (!columnDelimiter.isEmpty())
									val = val.replace(columnDelimiter, escapeCharacter + columnDelimiter);
								if (!newLine.isEmpty())
									val = val.replace(newLine, escapeCharacter + newLine);
							}
						}
						addStringElement(outBuffer, columnName, val);
					} else if (colObj instanceof BigDecimal) {
						addStringElement(outBuffer, columnName, ((BigDecimal) colObj).toString());
					} else {
						addStringElement(outBuffer, columnName, colObj.toString());
					}
				} else {
					if (csvOutput) {
						if (needToCloseQuote) {
							outBuffer.put(QUOTE_COMMA_AS_BYTE_ARRAY);
							needToCloseQuote = false;
						} else {
							outBuffer.put(COMMA_AS_BYTE_ARRAY);
						}
					}
				}
			}
			// Write closing quote or right curly bracket
			if (csvOutput && needToCloseQuote)
				outBuffer.put(QUOTE_AS_BYTE_ARRAY);
			if (!csvOutput && afterImage)
				outBuffer = addUtf8StringToByteBuffer(outBuffer, FIXED_RIGHT_CURLY);

		}

		// If the before image was processed, make sure the next data image is
		// treated as the after image
		if (!afterImage)
			afterImage = true;

		return outBuffer;
	}

	/**
	 * Add element to the output buffer, depending if it's CSV of JSON
	 */
	private void addStringElement(ByteBuffer outBuffer, String columnName, String data)
			throws DataTypeConversionException {
		if (csvOutput)
			outBuffer = addUtf8StringToByteBuffer(outBuffer, data);
		else
			outBuffer = addUtf8StringToByteBuffer(outBuffer, getJsonElement(columnName, data));
	}

	/**
	 * Return a ByteBuffer containing the appropriate null values for the row.
	 */
	public ByteBuffer formatNullImage(DataRecordIF image) throws DataTypeConversionException {
		ByteBuffer returnByteBuffer = null;
		if (csvOutput) {
			// There is a separate data formatter for each table, so a null
			// image is the same for each row, so just need to create it once
			if (csvNullImage == null) {
				String outString = "";
				if (image != null) {
					for (int i = 1; i <= image.getColumnCount() - NUM_TRAILING_COLUMNS; i++) {
						outString = outString + new String(COMMA_AS_BYTE_ARRAY);
					}
				}
				csvNullImage = ByteBuffer.wrap(getAsByteArrayInUtf8(outString));
				csvNullImage.position(csvNullImage.capacity());
			}
			returnByteBuffer = csvNullImage;
		} else {
			String outString = "";
			if (afterImage)
				outString = outString + FIXED_RIGHT_CURLY;
			returnByteBuffer = ByteBuffer.wrap(getAsByteArrayInUtf8(outString));
			returnByteBuffer.position(returnByteBuffer.capacity());
			afterImage = true; // Next image is the after image
		}
		return returnByteBuffer;
	}

	/**
	 * Return a ByteBuffer containing the journal control field values that are
	 * of interest.
	 * 
	 */
	public ByteBuffer formatJournalControlFields(ReplicationEventIF event, int opType)
			throws DataTypeConversionException {
		// Make sure that the JSON record is only closed after the full image
		// has been processed
		afterImage = false;

		// Determine the character to use to indicate the operation type
		char opChar = ' ';
		currentOpType = opType;
		switch (opType) {
		case DataStageDataFormatIF.INSERT_RECORD:
			opChar = SUB_RLA_INS_UPD;
			break;
		case DataStageDataFormatIF.DELETE_RECORD:
			opChar = SUB_RLA_DEL_NONE;
			break;
		case DataStageDataFormatIF.FULL_UPDATE_RECORD:
			opChar = SUB_RLA_NON_UPD;
			break;
		case DataStageDataFormatIF.BEFORE_UPDATE_RECORD:
			opChar = SUB_RLA_AUDIT_B4;
			break;
		case DataStageDataFormatIF.AFTER_UPDATE_RECORD:
			opChar = SUB_RLA_AUDIT;
			break;

		}
		String opString = "" + opChar;

		UserExitJournalHeader header = (UserExitJournalHeader) event.getJournalHeader();
		String commitIDString = header.getCommitID();

		// Retrieve and format the journal control timestamp
		String timestampString = "";
		if (overrideJournalControlTimestampFormat)
			timestampString = header.getDSOutputTimestamp().toString();
		else
			timestampString = new SimpleDateFormat(journalControlTimestampFormat).format(header.getDSOutputTimestamp());

		String outString = null;
		if (csvOutput) {
			outString = new String(QUOTE_AS_BYTE_ARRAY) + timestampString + new String(QUOTE_COMMA_QUOTE_AS_BYTE_ARRAY)
					+ commitIDString + new String(QUOTE_COMMA_QUOTE_AS_BYTE_ARRAY) + opChar
					+ new String(QUOTE_COMMA_QUOTE_AS_BYTE_ARRAY) + header.getUserName()
					+ new String(QUOTE_AS_BYTE_ARRAY);
		} else {
			// Compose the JSON string with audit columns, ending with ,"
			outString = new String(FIXED_LEFT_CURLY + getJsonElement("AUD_TIMESTAMP", timestampString) + FIXED_COMMA
					+ getJsonElement("AUD_CCID", commitIDString) + FIXED_COMMA + getJsonElement("AUD_ENTTYP", opString)
					+ FIXED_COMMA + getJsonElement("AUD_USER", header.getUserName()));
		}

		ByteBuffer retVal = ByteBuffer.allocate(outString.length());
		retVal = addUtf8StringToByteBuffer(retVal, outString);
		return retVal;
	}

	private String getJsonElement(String elementName, String elementValue) {
		return new String(FIXED_QUOTE + elementName + FIXED_QUOTE_COLON_QUOTE + elementValue + FIXED_QUOTE);
	}

	/**
	 * Indicate whether this table is being delivered to DataStage using flat
	 * files or by direct connect. As CDC for DataStage only supports flat
	 * files, the destination type is no longer relevant.
	 */
	public void setDestinationType(int destination) {
	}

	public void formatChangedRowFields(UserExitJournalHeader journalHeader, DataRecordIF rowDataImage,
			Map<String, Object> changeRecord, int opType) throws DataTypeConversionException {

	}

}
