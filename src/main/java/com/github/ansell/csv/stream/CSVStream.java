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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

/**
 * Implements streaming of CSV files for both parsing and writing using Java-8
 * Lambda functions such as {@link Consumer} and {@link BiFunction}..
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class CSVStream {

	/**
	 * Private constructor for static only class
	 */
	private CSVStream() {
	}

	/**
	 * Stream a CSV file from the given InputStream through the header
	 * validator, line checker, and if the line checker succeeds, send the
	 * checked/converted line to the consumer.
	 * 
	 * @param inputStream
	 *            The {@link InputStream} containing the CSV file.
	 * @param headerValidator
	 *            The validator of the header line. Throwing
	 *            IllegalArgumentException or other RuntimeExceptions causes the
	 *            parsing process to short-circuit after parsing the header
	 *            line, with a CSVStreamException being rethrown by this code.
	 * @param lineConverter
	 *            The validator and converter of lines, based on the header
	 *            line. If the lineChecker returns null, the line will not be
	 *            passed to the writer.
	 * @param resultConsumer
	 *            The consumer of the checked lines.
	 * @param <T>
	 *            The type of the results that will be created by the
	 *            lineChecker and pushed into the writer {@link Consumer}.
	 * @throws IOException
	 *             If an error occurred accessing the input.
	 * @throws CSVStreamException
	 *             If an error occurred validating the input.
	 */
	public static <T> void parse(final InputStream inputStream, final Consumer<List<String>> headerValidator,
			final BiFunction<List<String>, List<String>, T> lineConverter, final Consumer<T> resultConsumer)
			throws IOException, CSVStreamException {
		try (final InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);) {
			parse(inputStreamReader, headerValidator, lineConverter, resultConsumer);
		}
	}

	/**
	 * Stream a CSV file from the given Reader through the header validator,
	 * line checker, and if the line checker succeeds, send the
	 * checked/converted line to the consumer.
	 * 
	 * @param reader
	 *            The {@link Reader} containing the CSV file.
	 * @param headerValidator
	 *            The validator of the header line. Throwing
	 *            IllegalArgumentException or other RuntimeExceptions causes the
	 *            parsing process to short-circuit after parsing the header
	 *            line, with a CSVStreamException being rethrown by this code.
	 * @param lineConverter
	 *            The validator and converter of lines, based on the header
	 *            line. If the lineChecker returns null, the line will not be
	 *            passed to the writer.
	 * @param resultConsumer
	 *            The consumer of the checked lines.
	 * @param <T>
	 *            The type of the results that will be created by the
	 *            lineChecker and pushed into the writer {@link Consumer}.
	 * @throws IOException
	 *             If an error occurred accessing the input.
	 * @throws CSVStreamException
	 *             If an error occurred validating the input.
	 */
	public static <T> void parse(final Reader reader, final Consumer<List<String>> headerValidator,
			final BiFunction<List<String>, List<String>, T> lineConverter, final Consumer<T> resultConsumer)
			throws IOException, CSVStreamException {
		final CsvMapper mapper = new CsvMapper();
		mapper.enable(CsvParser.Feature.TRIM_SPACES);
		mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);

		List<String> headers = null;

		try (final MappingIterator<List<String>> it = mapper.readerFor(List.class).readValues(reader);) {
			while (it.hasNext()) {
				List<String> nextLine = it.next();
				if (headers == null) {
					headers = nextLine.stream().map(v -> v.trim()).map(v -> v.intern()).collect(Collectors.toList());
					try {
						headerValidator.accept(headers);
					} catch (final Exception e) {
						throw new CSVStreamException("Could not verify headers for csv file", e);
					}
				} else {
					if (nextLine.size() != headers.size()) {
						throw new CSVStreamException(
								"Line and header sizes were different: " + headers + " " + nextLine);
					}

					final T apply = lineConverter.apply(headers, nextLine);

					// Line checker returning null indicates that a value was
					// not found, and will not be sent to the consumer.
					if (apply != null) {
						resultConsumer.accept(apply);
					}
				}
			}
		} catch (Exception e) {
			throw new CSVStreamException(e);
		}

		if (headers == null) {
			throw new CSVStreamException("CSV file did not contain a valid header line");
		}
	}

	/**
	 * Returns a Jackson {@link SequenceWriter} which will write CSV lines to
	 * the given {@link OutputStream} using the headers provided.
	 * 
	 * @param outputStream
	 *            The writer which will receive the CSV file.
	 * @param header
	 *            The column headers that will be used by the returned Jackson
	 *            {@link SequenceWriter}.
	 * @return A Jackson {@link SequenceWriter} that can have
	 *         {@link SequenceWriter#write(Object)} called on it to emit CSV
	 *         lines to the given {@link OutputStream}.
	 * @throws IOException
	 *             If there is a problem writing the CSV header line to the
	 *             {@link OutputStream}.
	 */
	public static SequenceWriter newCSVWriter(final OutputStream outputStream, List<String> header) throws IOException {
		return newCSVWriter(outputStream, buildSchema(header));
	}

	/**
	 * Returns a Jackson {@link SequenceWriter} which will write CSV lines to
	 * the given {@link OutputStream} using the {@link CsvSchema}.
	 * 
	 * @param outputStream
	 *            The writer which will receive the CSV file.
	 * @param schema
	 *            The {@link CsvSchema} that will be used by the returned
	 *            Jackson {@link SequenceWriter}.
	 * @return A Jackson {@link SequenceWriter} that can have
	 *         {@link SequenceWriter#write(Object)} called on it to emit CSV
	 *         lines to the given {@link OutputStream}.
	 * @throws IOException
	 *             If there is a problem writing the CSV header line to the
	 *             {@link OutputStream}.
	 */
	public static SequenceWriter newCSVWriter(final OutputStream outputStream, CsvSchema schema) throws IOException {
		return new CsvMapper().writerWithDefaultPrettyPrinter().with(schema).forType(List.class)
				.writeValues(outputStream);
	}

	/**
	 * Returns a Jackson {@link SequenceWriter} which will write CSV lines to
	 * the given {@link Writer} using the headers provided.
	 * 
	 * @param writer
	 *            The writer which will receive the CSV file.
	 * @param header
	 *            The column headers that will be used by the returned Jackson
	 *            {@link SequenceWriter}.
	 * @return A Jackson {@link SequenceWriter} that can have
	 *         {@link SequenceWriter#write(Object)} called on it to emit CSV
	 *         lines to the given {@link Writer}.
	 * @throws IOException
	 *             If there is a problem writing the CSV header line to the
	 *             {@link Writer}.
	 */
	public static SequenceWriter newCSVWriter(final Writer writer, List<String> header) throws IOException {
		return newCSVWriter(writer, buildSchema(header));
	}

	/**
	 * Returns a Jackson {@link SequenceWriter} which will write CSV lines to
	 * the given {@link Writer} using the {@link CsvSchema}.
	 * 
	 * @param writer
	 *            The writer which will receive the CSV file.
	 * @param schema
	 *            The {@link CsvSchema} that will be used by the returned
	 *            Jackson {@link SequenceWriter}.
	 * @return A Jackson {@link SequenceWriter} that can have
	 *         {@link SequenceWriter#write(Object)} called on it to emit CSV
	 *         lines to the given {@link Writer}.
	 * @throws IOException
	 *             If there is a problem writing the CSV header line to the
	 *             {@link Writer}.
	 */
	public static SequenceWriter newCSVWriter(final Writer writer, CsvSchema schema) throws IOException {
		return new CsvMapper().writerWithDefaultPrettyPrinter().with(schema).forType(List.class).writeValues(writer);
	}

	/**
	 * Build a {@link CsvSchema} object using the given headers.
	 * 
	 * @param header
	 *            The list of strings in the header.
	 * @return A {@link CsvSchema} object including the given header items.
	 */
	public static CsvSchema buildSchema(List<String> header) {
		CsvSchema.Builder result = CsvSchema.builder();

		for (String nextHeader : header) {
			result = result.addColumn(nextHeader);
		}

		return result.setUseHeader(true).build();
	}

}
