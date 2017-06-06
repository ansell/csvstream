/*
 * Copyright (c) 2016, Peter Ansell
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * 
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.github.ansell.csv.stream;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csv.stream.CSVStream;

/**
 * Tests for {@link CSVStream}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class CSVStreamTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVIllegalHeader() throws Exception {
		thrown.expect(CSVStreamException.class);
		thrown.expectMessage("Could not verify headers for csv file");
		CSVStream.parse(new StringReader("Header1"), h -> {
			throw new IllegalArgumentException("Did not find header: Header2");
		}, (h, l) -> l, l -> {
		});
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.InputStream, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVEmptyInputStream() throws Exception {
		List<String> headers = new ArrayList<>();

		thrown.expect(CSVStreamException.class);
		thrown.expectMessage("CSV file did not contain a valid header line");
		CSVStream.parse(new ByteArrayInputStream(new byte[0]), h -> headers.addAll(h), (h, l) -> l, l -> {
		});
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.InputStream, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVInputStreamSingleRow() throws Exception {
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVStream.parse(new ByteArrayInputStream("TestHeader1\nTestValue1\n".getBytes(StandardCharsets.UTF_8)),
				h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVEmptyWriter() throws Exception {
		List<String> headers = new ArrayList<>();

		thrown.expect(CSVStreamException.class);
		thrown.expectMessage("CSV file did not contain a valid header line");
		CSVStream.parse(new StringReader(""), h -> headers.addAll(h), (h, l) -> l, l -> {
		});
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVSingleColumnMoreOnRow() throws Exception {
		List<String> headers = new ArrayList<>();

		thrown.expect(CSVStreamException.class);
		CSVStream.parse(new StringReader("Test1\nAnswer1,Answer2,Answer3"), h -> headers.addAll(h), (h, l) -> l, l -> {
		});
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVMultipleColumnsLessOnRow() throws Exception {
		List<String> headers = new ArrayList<>();

		thrown.expect(CSVStreamException.class);
		CSVStream.parse(new StringReader("Test1, Another2, Else3\nAnswer1"), h -> headers.addAll(h), (h, l) -> l, l -> {
		});
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVHeaderOnlySingleColumn() throws Exception {
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVStream.parse(new StringReader("Test1"), h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		assertEquals(1, headers.size());
		assertTrue(headers.contains("Test1"));
		assertEquals(0, lines.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVHeaderOnlyMultipleColumns() throws Exception {
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVStream.parse(new StringReader("Test1, Another2, Else3"), h -> headers.addAll(h), (h, l) -> l,
				l -> lines.add(l));
		assertEquals(3, headers.size());
		assertTrue(headers.contains("Test1"));
		assertTrue(headers.contains("Another2"));
		assertTrue(headers.contains("Else3"));
		assertEquals(0, lines.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVSingleRowSingleColumn() throws Exception {
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVStream.parse(new StringReader("Test1\nAnswer1"), h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		assertEquals(1, headers.size());
		assertTrue(headers.contains("Test1"));
		assertEquals(1, lines.size());
		assertTrue(lines.contains(Arrays.asList("Answer1")));
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVSingleRowMultipleColumns() throws Exception {
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVStream.parse(new StringReader("Test1, Another2, Else3\nAnswer1, Alternative2, Attempt3"),
				h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		assertEquals(3, headers.size());
		assertTrue(headers.contains("Test1"));
		assertTrue(headers.contains("Another2"));
		assertTrue(headers.contains("Else3"));
		assertEquals(1, lines.size());
		assertTrue(lines.contains(Arrays.asList("Answer1", "Alternative2", "Attempt3")));
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVMultipleRowsSingleColumn() throws Exception {
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVStream.parse(new StringReader("Test1\nAnswer1\nAnswer2\nAnswer3"), h -> headers.addAll(h), (h, l) -> l,
				l -> lines.add(l));
		assertEquals(1, headers.size());
		assertTrue(headers.contains("Test1"));
		assertEquals(3, lines.size());
		assertTrue(lines.contains(Arrays.asList("Answer1")));
		assertTrue(lines.contains(Arrays.asList("Answer2")));
		assertTrue(lines.contains(Arrays.asList("Answer3")));
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVMultipleRowsMultipleColumns() throws Exception {
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVStream.parse(
				new StringReader(
						"Test1, Another2, Else3\nAnswer1, Alternative2, Attempt3\nAnswer4, Alternative5, Attempt6\nAnswer7, Alternative8, Attempt9"),
				h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		assertEquals(3, headers.size());
		assertTrue(headers.contains("Test1"));
		assertTrue(headers.contains("Another2"));
		assertTrue(headers.contains("Else3"));
		assertEquals(3, lines.size());
		assertTrue(lines.contains(Arrays.asList("Answer1", "Alternative2", "Attempt3")));
		assertTrue(lines.contains(Arrays.asList("Answer4", "Alternative5", "Attempt6")));
		assertTrue(lines.contains(Arrays.asList("Answer7", "Alternative8", "Attempt9")));
	}

	@Test
	public final void testWriteFullCode() throws Exception {
		List<String> headers = Arrays.asList("TestHeader1", "TestHeader2");
		List<List<String>> dataSource = Arrays.asList();
		// Or alternatively,
		// List<List<String>> dataSource =
		// Arrays.asList(Arrays.asList("TestValue1", "TestValue2"));
		java.io.Writer writer = new StringWriter();
		CsvSchema.Builder builder = CsvSchema.builder();
		for (String nextHeader : headers) {
			builder = builder.addColumn(nextHeader);
		}
		CsvSchema schema = builder.setUseHeader(true).build();
		try (SequenceWriter csvWriter = new CsvMapper().writerWithDefaultPrettyPrinter().with(schema)
				.forType(List.class).writeValues(writer);) {
			for (List<String> nextRow : dataSource) {
				csvWriter.write(nextRow);
			}
			// Check to see whether dataSource is empty
			// and if so write a single empty list to trigger header output
			if (dataSource.isEmpty()) {
				csvWriter.write(Arrays.asList());
			}
		}
		System.out.println(writer.toString());
	}

	@Test
	public final void testWriteEmptySingle() throws Exception {
		List<String> headers = Arrays.asList("TestHeader1");
		StringWriter writer = new StringWriter();
		CSVStream.newCSVWriter(writer, headers).write(Arrays.asList());
		System.out.println(writer.toString());
		assertEquals("TestHeader1\n", writer.toString());
	}

	@Test
	public final void testWriteEmptySingleOutputStream() throws Exception {
		List<String> headers = Arrays.asList("TestHeader1");
		ByteArrayOutputStream writer = new ByteArrayOutputStream();
		CSVStream.newCSVWriter(writer, headers).write(Arrays.asList());
		System.out.println(writer.toString());
		assertEquals("TestHeader1\n", writer.toString("UTF-8"));
	}

	@Test
	public final void testWriteEmptyAll() throws Exception {
		List<String> headers = Arrays.asList("TestHeader1");
		StringWriter writer = new StringWriter();
		CSVStream.newCSVWriter(writer, headers).writeAll(Arrays.asList(Arrays.asList()));
		System.out.println(writer.toString());
		assertEquals("TestHeader1\n", writer.toString());
	}

	@Test
	public final void testWriteSingleEmptyString() throws Exception {
		List<String> headers = Arrays.asList("TestHeader1");
		StringWriter writer = new StringWriter();
		CSVStream.newCSVWriter(writer, headers).writeAll(Arrays.asList(Arrays.asList("")));
		System.out.println(writer.toString());
		assertEquals("TestHeader1\n\n", writer.toString());

		AtomicBoolean headersGood = new AtomicBoolean(false);
		AtomicBoolean lineGood = new AtomicBoolean(false);
		CSVStream.parse(new StringReader(writer.toString()), h -> {
			if (h.size() == 1 && h.contains("TestHeader1")) {
				headersGood.set(true);
			}
		}, (h, l) -> {
			if (l.size() == 1 && l.contains("")) {
				lineGood.set(true);
			}
			return l;
		}, l -> {
		});

		assertTrue("Headers were not recognised", headersGood.get());
		assertTrue("Line was not recognised", lineGood.get());
	}

	@Test
	public final void testWriteStreamSingleValue() throws Exception {
		StringWriter writer = new StringWriter();
		CSVStream.write(writer, Stream.of(Arrays.asList("TestValue1")), Arrays.asList("TestHeader1"), (h, o) -> o);

		System.out.println(writer.toString());
		assertEquals("TestHeader1\nTestValue1\n", writer.toString());

		AtomicBoolean headersGood = new AtomicBoolean(false);
		AtomicBoolean lineGood = new AtomicBoolean(false);
		CSVStream.parse(new StringReader(writer.toString()), h -> {
			if (h.size() == 1 && h.contains("TestHeader1")) {
				headersGood.set(true);
			}
		}, (h, l) -> {
			if (l.size() == 1 && l.contains("TestValue1")) {
				lineGood.set(true);
			}
			return l;
		}, l -> {
		});

		assertTrue("Headers were not recognised", headersGood.get());
		assertTrue("Line was not recognised", lineGood.get());
	}

	/**
	 * Test method for
	 * {@link CSVStream#write(Writer, Stream, List, java.util.function.BiFunction)}
	 * .
	 */
	@Test
	public final void testWriteStreamErrorConvertingObject() throws Exception {
		StringWriter writer = new StringWriter();
		thrown.expect(CSVStreamException.class);
		CSVStream.write(writer, Stream.of(Arrays.asList("TestValue1")), Arrays.asList("TestHeader1"), (h, o) -> {
			if (o.size() == 1) {
				throw new IllegalArgumentException("Failing to test error handling");
			}
			return o;
		});
		fail("Should not have successfully written the document. Output was: " + writer.toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVNegativeHeaderCount() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Header line count must be non-negative.");
		CSVStream.parse(new StringReader("Test"), h -> {
		}, (h, l) -> l, l -> {
		}, null, -1);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVZeroHeadersNoSubstitutes() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("If there are no header lines, a substitute set of headers must be defined.");
		CSVStream.parse(new StringReader("Test"), h -> {
		}, (h, l) -> l, l -> {
		}, null, 0);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVFailSubstitutedHeaderValidation() throws Exception {
		thrown.expect(CSVStreamException.class);
		thrown.expectMessage("Could not verify substituted headers for csv file");
		CSVStream.parse(new StringReader("TestHeaderWhichShouldNotBeSeen"), h -> { 
			throw new RuntimeException("Testing failure of validation for substituted headers: " + h.toString());
		}, (h, l) -> l, l -> {
		}, Arrays.asList("TestSubstitutedHeader"), 0);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVFailSubstitutedHeaderValidationOther() throws Exception {
		thrown.expect(CSVStreamException.class);
		thrown.expectMessage("Could not verify substituted headers for csv file");
		CSVStream.parse(new StringReader("TestHeaderWhichShouldNotBeSeen"), h -> { 
			throw new RuntimeException("Testing failure of validation for substituted headers: " + h.toString());
		}, (h, l) -> l, l -> {
		}, Arrays.asList("TestSubstitutedHeader"), 1);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVZeroHeadersWithSubstitutesValid() throws Exception {
		
		AtomicBoolean headersGood = new AtomicBoolean(false);
		AtomicBoolean lineGood = new AtomicBoolean(false);
		CSVStream.parse(new StringReader("TestValue1"), h -> {
			if (h.size() == 1 && h.contains("TestHeader1")) {
				headersGood.set(true);
			}
		}, (h, l) -> {
			if (l.size() == 1 && l.contains("TestValue1")) {
				lineGood.set(true);
			}
			return l;
		}, l -> {
		}, Arrays.asList("TestHeader1"), 0);

		assertTrue("Headers were not recognised", headersGood.get());
		assertTrue("Line was not recognised", lineGood.get());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVZeroHeadersWithSubstitutesOverrideValid() throws Exception {
		
		AtomicBoolean headersGood = new AtomicBoolean(false);
		AtomicBoolean lineGood = new AtomicBoolean(false);
		AtomicBoolean foundLine = new AtomicBoolean(false);
		AtomicBoolean lineError = new AtomicBoolean(false);
		CSVStream.parse(new StringReader("TestShouldNotSeeThisHeader1\nTestValue1"), h -> {
			if (h.size() == 1 && h.contains("TestHeader1")) {
				headersGood.set(true);
			}
		}, (h, l) -> {
			if (foundLine.compareAndSet(false, true) && l.size() == 1 && l.contains("TestValue1")) {
				lineGood.set(true);
			} else {
				lineError.set(true);
			}
			return l;
		}, l -> {
		}, Arrays.asList("TestHeader1"), 1);

		assertTrue("Headers were not recognised", headersGood.get());
		assertTrue("Line was not recognised", lineGood.get());
		assertTrue("Line was not found", foundLine.get());
		assertFalse("Too many lines", lineError.get());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVZeroHeadersWithSubstitutesOverridingMultipleValid() throws Exception {
		
		AtomicBoolean headersGood = new AtomicBoolean(false);
		AtomicBoolean lineGood = new AtomicBoolean(false);
		AtomicBoolean foundLine = new AtomicBoolean(false);
		AtomicBoolean lineError = new AtomicBoolean(false);
		CSVStream.parse(new StringReader("TestShouldNotSeeThisHeader1\nTestShouldDefinitelyNotSeeThisHeader2\nYetAnotherHiddenHeader3\nTestValue1"), h -> {
			if (h.size() == 1 && h.contains("TestHeader1")) {
				headersGood.set(true);
			}
		}, (h, l) -> {
			if (foundLine.compareAndSet(false, true) && l.size() == 1 && l.contains("TestValue1")) {
				lineGood.set(true);
			} else {
				lineError.set(true);
			}
			return l;
		}, l -> {
		}, Arrays.asList("TestHeader1"), 3);

		assertTrue("Headers were not recognised", headersGood.get());
		assertTrue("Line was not recognised", lineGood.get());
		assertTrue("Line was not found", foundLine.get());
		assertFalse("Too many lines", lineError.get());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVThreeHeadersNoSubstitutes() throws Exception {
		
		AtomicBoolean headersGood = new AtomicBoolean(false);
		AtomicBoolean lineGood = new AtomicBoolean(false);
		AtomicBoolean foundLine = new AtomicBoolean(false);
		AtomicBoolean lineError = new AtomicBoolean(false);
		CSVStream.parse(new StringReader("TestHeader1\nTestShouldDefinitelyNotSeeThisHeader2\nYetAnotherHiddenHeader3\nTestValue1"), h -> {
			if (h.size() == 1 && h.contains("TestHeader1")) {
				headersGood.set(true);
			}
		}, (h, l) -> {
			if (foundLine.compareAndSet(false, true) && l.size() == 1 && l.contains("TestValue1")) {
				lineGood.set(true);
			} else {
				lineError.set(true);
			}
			return l;
		}, l -> {
		}, null, 3);

		assertTrue("Headers were not recognised", headersGood.get());
		assertTrue("Line was not recognised", lineGood.get());
		assertTrue("Line was not found", foundLine.get());
		assertFalse("Too many lines", lineError.get());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVUnescapedNewLineRFC4180() throws Exception {
		
		AtomicBoolean headersGood = new AtomicBoolean(false);
		AtomicBoolean lineGood = new AtomicBoolean(false);
		AtomicBoolean foundLine = new AtomicBoolean(false);
		AtomicBoolean lineError = new AtomicBoolean(false);
		CSVStream.parse(new StringReader("TestHeader1\n\"Test\nValue1\""), h -> {
			if (h.size() == 1 && h.contains("TestHeader1")) {
				headersGood.set(true);
			}
		}, (h, l) -> {
			if (foundLine.compareAndSet(false, true) && l.size() == 1 && l.contains("Test\nValue1")) {
				lineGood.set(true);
			} else {
				lineError.set(true);
			}
			return l;
		}, l -> {
		}, null, 1);

		assertTrue("Headers were not recognised", headersGood.get());
		assertTrue("Line was not recognised", lineGood.get());
		assertTrue("Line was not found", foundLine.get());
		assertFalse("Too many lines", lineError.get());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVUnescapedNewLineRFC4180Header() throws Exception {
		
		AtomicBoolean headersGood = new AtomicBoolean(false);
		AtomicBoolean lineGood = new AtomicBoolean(false);
		AtomicBoolean foundLine = new AtomicBoolean(false);
		AtomicBoolean lineError = new AtomicBoolean(false);
		CSVStream.parse(new StringReader("\"Test\nHeader1\"\n\"TestValue1\""), h -> {
			if (h.size() == 1 && h.contains("Test\nHeader1")) {
				headersGood.set(true);
			}
		}, (h, l) -> {
			if (foundLine.compareAndSet(false, true) && l.size() == 1 && l.contains("TestValue1")) {
				lineGood.set(true);
			} else {
				lineError.set(true);
			}
			return l;
		}, l -> {
		}, null, 1);

		assertTrue("Headers were not recognised", headersGood.get());
		assertTrue("Line was not recognised", lineGood.get());
		assertTrue("Line was not found", foundLine.get());
		assertFalse("Too many lines", lineError.get());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVCustomQuoteCharacter() throws Exception {
		
		AtomicBoolean headersGood = new AtomicBoolean(false);
		AtomicBoolean lineGood = new AtomicBoolean(false);
		AtomicBoolean foundLine = new AtomicBoolean(false);
		AtomicBoolean lineError = new AtomicBoolean(false);
		CsvMapper mapper = CSVStream.defaultMapper();
		CsvSchema schema = CsvSchema.builder().setQuoteChar('\'').build();
		CSVStream.parse(new StringReader("'Test\nHeader1'\n'TestValue1'"), h -> {
			if (h.size() == 1 && h.contains("Test\nHeader1")) {
				headersGood.set(true);
			}
		}, (h, l) -> {
			if (foundLine.compareAndSet(false, true) && l.size() == 1 && l.contains("TestValue1")) {
				lineGood.set(true);
			} else {
				lineError.set(true);
			}
			return l;
		}, l -> {
		}, null, 1, mapper, schema);

		assertTrue("Headers were not recognised", headersGood.get());
		assertTrue("Line was not recognised", lineGood.get());
		assertTrue("Line was not found", foundLine.get());
		assertFalse("Too many lines", lineError.get());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVCustomQuoteCharacterInside() throws Exception {
		
		AtomicBoolean headersGood = new AtomicBoolean(false);
		AtomicBoolean lineGood = new AtomicBoolean(false);
		AtomicBoolean foundLine = new AtomicBoolean(false);
		AtomicBoolean lineError = new AtomicBoolean(false);
		CsvMapper mapper = CSVStream.defaultMapper();
		CsvSchema schema = CsvSchema.builder().setQuoteChar('\'').build();
		CSVStream.parse(new StringReader("'Test''\nHeader1'\n'TestValue1'"), h -> {
			if (h.size() == 1 && h.contains("Test'\nHeader1")) {
				headersGood.set(true);
			}
		}, (h, l) -> {
			if (foundLine.compareAndSet(false, true) && l.size() == 1 && l.contains("TestValue1")) {
				lineGood.set(true);
			} else {
				lineError.set(true);
			}
			return l;
		}, l -> {
		}, null, 1, mapper, schema);

		assertTrue("Headers were not recognised", headersGood.get());
		assertTrue("Line was not recognised", lineGood.get());
		assertTrue("Line was not found", foundLine.get());
		assertFalse("Too many lines", lineError.get());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVComment() throws Exception {
		
		AtomicBoolean headersGood = new AtomicBoolean(false);
		AtomicBoolean lineGood = new AtomicBoolean(false);
		AtomicBoolean foundLine = new AtomicBoolean(false);
		AtomicBoolean lineError = new AtomicBoolean(false);
		// The default mapper skips comment lines
		CsvMapper mapper = CSVStream.defaultMapper();
		CsvSchema schema = CsvSchema.builder().setQuoteChar('\'').build();
		CSVStream.parse(new StringReader("'Test''\nHeader1'\n#A Comment that should be skipped\n'TestValue1'"), h -> {
			if (h.size() == 1 && h.contains("Test'\nHeader1")) {
				headersGood.set(true);
			}
		}, (h, l) -> {
			if (foundLine.compareAndSet(false, true) && l.size() == 1 && l.contains("TestValue1")) {
				lineGood.set(true);
			} else {
				lineError.set(true);
			}
			return l;
		}, l -> {
		}, null, 1, mapper, schema);

		assertTrue("Headers were not recognised", headersGood.get());
		assertTrue("Line was not recognised", lineGood.get());
		assertTrue("Line was not found", foundLine.get());
		assertFalse("Too many lines", lineError.get());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVQuoteAndEscapeChanged() throws Exception {
		
		AtomicBoolean headersGood = new AtomicBoolean(false);
		AtomicBoolean lineGood = new AtomicBoolean(false);
		AtomicBoolean foundLine = new AtomicBoolean(false);
		AtomicBoolean lineError = new AtomicBoolean(false);
		// The default mapper skips comment lines
		CsvMapper mapper = CSVStream.defaultMapper();
		CsvSchema schema = CsvSchema.builder().setQuoteChar('\'').setEscapeChar('\'').build();
		CSVStream.parse(new StringReader("'Test''\nHeader1'\n'Test''Value1'"), h -> {
			if (h.size() == 1 && h.contains("Test'\nHeader1")) {
				headersGood.set(true);
			}
		}, (h, l) -> {
			if (foundLine.compareAndSet(false, true) && l.size() == 1 && l.contains("Test'Value1")) {
				lineGood.set(true);
			} else {
				lineError.set(true);
			}
			return l;
		}, l -> {
		}, null, 1, mapper, schema);

		assertTrue("Headers were not recognised", headersGood.get());
		assertTrue("Line was not recognised", lineGood.get());
		assertTrue("Line was not found", foundLine.get());
		assertFalse("Too many lines", lineError.get());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer, List, int)}
	 * .
	 */
	@Test
	public final void testStreamCSVQuoteAndEscapeChangedDifferent() throws Exception {
		
		AtomicBoolean headersGood = new AtomicBoolean(false);
		AtomicBoolean lineGood = new AtomicBoolean(false);
		AtomicBoolean foundLine = new AtomicBoolean(false);
		AtomicBoolean lineError = new AtomicBoolean(false);
		// The default mapper skips comment lines
		CsvMapper mapper = CSVStream.defaultMapper();
		CsvSchema schema = CsvSchema.builder().setQuoteChar('\'').setEscapeChar('"').build();
		CSVStream.parse(new StringReader("'Test\"\"\nHeader1'\n'Test\"\"Value1'"), h -> {
			if (h.size() == 1 && h.contains("Test\"\nHeader1")) {
				headersGood.set(true);
			}
		}, (h, l) -> {
			if (foundLine.compareAndSet(false, true) && l.size() == 1 && l.contains("Test\"Value1")) {
				lineGood.set(true);
			} else {
				lineError.set(true);
			}
			return l;
		}, l -> {
		}, null, 1, mapper, schema);

		assertTrue("Headers were not recognised", headersGood.get());
		assertTrue("Line was not recognised", lineGood.get());
		assertTrue("Line was not found", foundLine.get());
		assertFalse("Too many lines", lineError.get());
	}

}
