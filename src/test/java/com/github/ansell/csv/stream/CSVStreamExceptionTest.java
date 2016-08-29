/**
 * 
 */
package com.github.ansell.csv.stream;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link CSVStreamException}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class CSVStreamExceptionTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();
	
	/**
	 * Test method for {@link com.github.ansell.csv.stream.CSVStreamException#CSVStreamException()}.
	 */
	@Test
	public final void testCSVStreamException() {
		thrown.expect(CSVStreamException.class);
		throw new CSVStreamException();
	}

	/**
	 * Test method for {@link com.github.ansell.csv.stream.CSVStreamException#CSVStreamException(java.lang.String)}.
	 */
	@Test
	public final void testCSVStreamExceptionString() {
		thrown.expectMessage("Test message");
		thrown.expect(CSVStreamException.class);
		throw new CSVStreamException("Test message");
	}

	/**
	 * Test method for {@link com.github.ansell.csv.stream.CSVStreamException#CSVStreamException(java.lang.Throwable)}.
	 */
	@Test
	public final void testCSVStreamExceptionThrowable() {
		RuntimeException cause = new RuntimeException("Test exception cause");
		thrown.expectMessage("Test exception cause");
		thrown.expect(CSVStreamException.class);
		throw new CSVStreamException(cause);
	}

	/**
	 * Test method for {@link com.github.ansell.csv.stream.CSVStreamException#CSVStreamException(java.lang.String, java.lang.Throwable)}.
	 */
	@Test
	public final void testCSVStreamExceptionStringThrowable() {
		RuntimeException cause = new RuntimeException("Test exception cause");
		thrown.expectMessage("Test message");
		thrown.expect(CSVStreamException.class);
		throw new CSVStreamException("Test message", cause);
	}

	/**
	 * Test method for {@link com.github.ansell.csv.stream.CSVStreamException#CSVStreamException(java.lang.String, java.lang.Throwable, boolean, boolean)}.
	 */
	@Test
	public final void testCSVStreamExceptionStringThrowableBooleanBoolean() {
		RuntimeException cause = new RuntimeException("Test exception cause");
		thrown.expectMessage("Test message");
		thrown.expect(CSVStreamException.class);
		throw new CSVStreamException("Test message", cause, true, true);
	}

}
