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
import com.datamirror.ts.target.publication.userexit.datastage.DataStageDataFormatIF;
import com.datamirror.ts.util.trace.Trace;

/**
 * 
 * Format the data suitable for the DataStage sequential file reader and column
 * importer stages.
 *
 */
public class FlatFileDataFormat implements DataStageDataFormatIF {
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

	private SimpleDateFormat outDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat outDateOnlyFormat = new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat outTimeOnlyFormat = new SimpleDateFormat("HH:mm:ss");

	private int destinationType; // Is this flat file or direct connect?

	private int clobTruncationPoint;
	private int blobTruncationPoint;

	/**
	 * Remember the truncation points.
	 */
	public void setLobTruncationPoint(int maxClobLengthInChars, int maxBlobLengthInBytes) {
		clobTruncationPoint = maxClobLengthInChars;
		blobTruncationPoint = maxBlobLengthInBytes;
	}

	ByteBuffer outBuffer = ByteBuffer.allocate(BYTE_BUFFER_AUTO_INCREMENT_SIZE);

	private byte[] COMMA_AS_BYTE_ARRAY = getAsByteArrayInUtf8(",");
	private byte[] QUOTE_AS_BYTE_ARRAY = getAsByteArrayInUtf8("\"");
	private byte[] COMMA_QUOTE_AS_BYTE_ARRAY = getAsByteArrayInUtf8(",\"");
	private byte[] QUOTE_COMMA_QUOTE_AS_BYTE_ARRAY = getAsByteArrayInUtf8("\",\"");
	private byte[] QUOTE_COMMA_AS_BYTE_ARRAY = getAsByteArrayInUtf8("\",");
	private byte[] ZERO_AS_BYTE_ARRAY = getAsByteArrayInUtf8("0");
	private byte[] ONE_AS_BYTE_ARRAY = getAsByteArrayInUtf8("1");

	private String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd hh:mm:ss";
	private String DEFAULT_DELIMITER = ",";
	private String DEFAULT_QUOTE = "\"";
	private String DEFAULT_NEW_LINE = "\n";

	private String datetimeFormat = DEFAULT_DATETIME_FORMAT;
	private String newLine = DEFAULT_NEW_LINE;
	String delimiter = DEFAULT_DELIMITER;
	String quote = DEFAULT_QUOTE;

	public FlatFileDataFormat() {

		loadConfigurationProperties();

		COMMA_AS_BYTE_ARRAY = getAsByteArrayInUtf8(delimiter);
		QUOTE_AS_BYTE_ARRAY = getAsByteArrayInUtf8(quote);
		COMMA_QUOTE_AS_BYTE_ARRAY = getAsByteArrayInUtf8(delimiter + quote);
		QUOTE_COMMA_QUOTE_AS_BYTE_ARRAY = getAsByteArrayInUtf8(quote + delimiter + quote);
		QUOTE_COMMA_AS_BYTE_ARRAY = getAsByteArrayInUtf8(quote + delimiter);

	}

	/*
	 * Load the configuration from the properties file found in the classpath
	 */
	private void loadConfigurationProperties() {

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

			datetimeFormat = prop.getProperty("datetimeFormat", DEFAULT_DATETIME_FORMAT);
			delimiter = prop.getProperty("delimiter", DEFAULT_DELIMITER);
			quote = prop.getProperty("quote", DEFAULT_QUOTE);
			newLine = prop.getProperty("newLine", DEFAULT_NEW_LINE);

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

	/**
	 * Create a string containing the data images for the row.
	 */
	public ByteBuffer formatDataImage(DataRecordIF image) throws DataTypeConversionException {
		boolean needToCloseQuote = false;
		outBuffer.position(0);

		if (image != null) {
			for (int i = 1; i <= image.getColumnCount() - NUM_TRAILING_COLUMNS; i++) {
				Object colObj = image.getObject(i);

				// For NULL values, we just leave the field empty
				if (colObj != null) {
					// For performance, we have this wacky logic to only do one
					// add of stuff between columns
					if (needToCloseQuote) {
						outBuffer.put(QUOTE_COMMA_QUOTE_AS_BYTE_ARRAY);

					} else {
						outBuffer.put(COMMA_QUOTE_AS_BYTE_ARRAY);
					}
					needToCloseQuote = true;

					if (colObj instanceof Time) {
						outBuffer = addUtf8StringToByteBuffer(outBuffer, outTimeOnlyFormat.format((Time) colObj));
					} else if (colObj instanceof Timestamp) {
						outBuffer = addUtf8StringToByteBuffer(outBuffer, outDateFormat.format((Timestamp) colObj));
					} else if (colObj instanceof Date) // This must be checked
														// after Time,
														// Timestamp, as such
														// objects are also Date
														// objects
					{
						outBuffer = addUtf8StringToByteBuffer(outBuffer, outDateOnlyFormat.format((Date) colObj));
					} else if (colObj instanceof byte[]) {
						byte[] val = (byte[]) colObj;
						if (val.length > blobTruncationPoint) {
							byte[] truncVal = new byte[blobTruncationPoint];
							ByteBuffer truncBuffer = ByteBuffer.wrap(truncVal);
							truncBuffer.put(val, 0, blobTruncationPoint);
							val = truncVal;
						}
						// If this is a Direct Connect, then double up the
						// quotes
						if (destinationType == DataStageDataFormatIF.DIRECT_CONNECT) {
							for (int j = 0; j < val.length; j++) {
								if (val[j] == QUOTE_AS_BYTE_ARRAY[0]) {
									// double up the quote
									outBuffer = addByteToByteBuffer(outBuffer, QUOTE_AS_BYTE_ARRAY[0]);
								}
								outBuffer = addByteToByteBuffer(outBuffer, val[j]);

							}

						} else {
							outBuffer = addBytesToByteBuffer(outBuffer, val);
						}
					} else if (colObj instanceof Boolean) {
						if (((Boolean) colObj).booleanValue()) {
							outBuffer = addBytesToByteBuffer(outBuffer, ONE_AS_BYTE_ARRAY);
						} else {
							outBuffer = addBytesToByteBuffer(outBuffer, ZERO_AS_BYTE_ARRAY);
						}
					} else if (colObj instanceof String) {
						String val = ((String) colObj);
						if (val.length() > clobTruncationPoint) {
							val = val.substring(0, clobTruncationPoint);
						}

						if (newLine.equals(DEFAULT_NEW_LINE)) {
							outBuffer = addUtf8StringToByteBuffer(outBuffer, val);
						} else {
							outBuffer = addUtf8StringToByteBuffer(outBuffer,
									val.replace("\r", "").replace("\n", newLine));
						}

					} else if (colObj instanceof BigDecimal) {
						outBuffer = addUtf8StringToByteBuffer(outBuffer, ((BigDecimal) colObj).toString()); // Use
																											// toPlainString
																											// for
																											// Java
																											// 1.5

					} else {
						outBuffer = addUtf8StringToByteBuffer(outBuffer, colObj.toString());
					}

				} else {
					if (needToCloseQuote) {
						outBuffer.put(QUOTE_COMMA_AS_BYTE_ARRAY);
						needToCloseQuote = false;

					} else {
						outBuffer.put(COMMA_AS_BYTE_ARRAY);
					}

				}

			}
			if (needToCloseQuote) {
				outBuffer.put(QUOTE_AS_BYTE_ARRAY);
			}

		}

		return outBuffer;
	}

	ByteBuffer nullImage = null;

	/**
	 * Return a ByteBuffer containing the appropriate null values for the row.
	 */
	public ByteBuffer formatNullImage(DataRecordIF image) throws DataTypeConversionException {
		// There is a separate data formatter for each table, so a null image is
		// the same for each row, so just need to create it once
		if (nullImage == null) {
			String outString = "";
			if (image != null) {
				for (int i = 1; i <= image.getColumnCount() - NUM_TRAILING_COLUMNS; i++) {
					outString = outString + new String(COMMA_AS_BYTE_ARRAY);
				}
			}
			nullImage = ByteBuffer.wrap(getAsByteArrayInUtf8(outString));
			nullImage.position(nullImage.capacity());
		}
		return nullImage;
	}

	/**
	 * Return a ByteBuffer containing the journal control field values that are
	 * of interest.
	 * 
	 */
	public ByteBuffer formatJournalControlFields(ReplicationEventIF event, int opType)
			throws DataTypeConversionException {
		// Determine the character to use to indicate the operation type
		char opChar = ' ';
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

		UserExitJournalHeader header = (UserExitJournalHeader) event.getJournalHeader();
		// String timestampString = header.getDSOutputTimestampStr();
		String commitIDString = header.getCommitID();

		String timestampString = new SimpleDateFormat(datetimeFormat).format(header.getDSOutputTimestamp());

		String outString = new String(QUOTE_AS_BYTE_ARRAY) + timestampString
				+ new String(QUOTE_COMMA_QUOTE_AS_BYTE_ARRAY) + commitIDString
				+ new String(QUOTE_COMMA_QUOTE_AS_BYTE_ARRAY) + opChar + new String(QUOTE_COMMA_QUOTE_AS_BYTE_ARRAY)
				+ header.getUserName() + new String(QUOTE_AS_BYTE_ARRAY);

		ByteBuffer retVal = ByteBuffer.allocate(1000);
		retVal = addUtf8StringToByteBuffer(retVal, outString);
		return retVal;
	}

	/**
	 * Indicate whether this table is being delivered to DataStage using flat
	 * files or by direct connect.
	 * 
	 * @param destination
	 *            indicates the destination type
	 */
	public void setDestinationType(int destination) {
		destinationType = destination;
	}

	public void formatChangedRowFields(UserExitJournalHeader journalHeader, DataRecordIF rowDataImage,
			Map<String, Object> changeRecord, int opType) throws DataTypeConversionException {

	}

}
