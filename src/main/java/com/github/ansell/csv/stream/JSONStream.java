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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType;

/**
 * Implements streaming of JSON files for parsing using Java-8 Lambda functions
 * such as {@link Consumer} and {@link BiFunction}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class JSONStream {

	public static final int DEFAULT_HEADER_COUNT = 1;

	/**
	 * Private constructor for static only class
	 */
	private JSONStream() {
	}

	/**
	 * Stream a JSON file from the given InputStream through the header validator,
	 * line checker, and if the line checker succeeds, send the checked/converted
	 * line to the consumer.
	 * 
	 * @param inputStream
	 *            The {@link InputStream} containing the JSON file.
	 * @param headersValidator
	 *            The validator of the header line. Throwing
	 *            IllegalArgumentException or other RuntimeExceptions causes the
	 *            parsing process to short-circuit after parsing the header line,
	 *            with a CSVStreamException being rethrown by this code.
	 * @param lineConverter
	 *            The validator and converter of lines, based on the header line. If
	 *            the lineChecker returns null, the line will not be passed to the
	 *            writer.
	 * @param resultConsumer
	 *            The consumer of the checked lines.
	 * @param <T>
	 *            The type of the results that will be created by the lineChecker
	 *            and pushed into the writer {@link Consumer}.
	 * @throws IOException
	 *             If an error occurred accessing the input.
	 * @throws CSVStreamException
	 *             If an error occurred validating the input.
	 */
	public static <T> void parse(final InputStream inputStream, final Consumer<List<String>> headersValidator,
			final BiFunction<List<String>, List<String>, T> lineConverter, final Consumer<T> resultConsumer)
			throws IOException, CSVStreamException {
		try (final Reader inputStreamReader = new BufferedReader(
				new InputStreamReader(inputStream, StandardCharsets.UTF_8));) {
			parse(inputStreamReader, headersValidator, lineConverter, resultConsumer);
		}
	}

	/**
	 * Stream a CSV file from the given Reader through the header validator, line
	 * checker, and if the line checker succeeds, send the checked/converted line to
	 * the consumer.
	 * 
	 * @param reader
	 *            The {@link Reader} containing the CSV file.
	 * @param headersValidator
	 *            The validator of the header line. Throwing
	 *            IllegalArgumentException or other RuntimeExceptions causes the
	 *            parsing process to short-circuit after parsing the header line,
	 *            with a CSVStreamException being rethrown by this code.
	 * @param lineConverter
	 *            The validator and converter of lines, based on the header line. If
	 *            the lineChecker returns null, the line will not be passed to the
	 *            writer.
	 * @param resultConsumer
	 *            The consumer of the checked lines.
	 * @param <T>
	 *            The type of the results that will be created by the lineChecker
	 *            and pushed into the writer {@link Consumer}.
	 * @throws IOException
	 *             If an error occurred accessing the input.
	 * @throws CSVStreamException
	 *             If an error occurred validating the input.
	 */
	public static <T> void parse(final Reader reader, final Consumer<List<String>> headersValidator,
			final BiFunction<List<String>, List<String>, T> lineConverter, final Consumer<T> resultConsumer)
			throws IOException, CSVStreamException {
		parse(reader, headersValidator, lineConverter, resultConsumer, null);
	}

	/**
	 * Stream a CSV file from the given Reader through the header validator, line
	 * checker, and if the line checker succeeds, send the checked/converted line to
	 * the consumer.
	 * 
	 * @param reader
	 *            The {@link Reader} containing the CSV file.
	 * @param headersValidator
	 *            The validator of the header line. Throwing
	 *            IllegalArgumentException or other RuntimeExceptions causes the
	 *            parsing process to short-circuit after parsing the header line,
	 *            with a CSVStreamException being rethrown by this code.
	 * @param lineConverter
	 *            The validator and converter of lines, based on the header line. If
	 *            the lineChecker returns null, the line will not be passed to the
	 *            writer.
	 * @param resultConsumer
	 *            The consumer of the checked lines.
	 * @param substituteHeaders
	 *            A substitute set of headers or null to use the headers from the
	 *            file. If this is null the first line of the file will be used.
	 * @param <T>
	 *            The type of the results that will be created by the lineChecker
	 *            and pushed into the writer {@link Consumer}.
	 * @throws IOException
	 *             If an error occurred accessing the input.
	 * @throws CSVStreamException
	 *             If an error occurred validating the input.
	 */
	public static <T> void parse(final Reader reader, final Consumer<List<String>> headersValidator,
			final BiFunction<List<String>, List<String>, T> lineConverter, final Consumer<T> resultConsumer,
			final List<String> substituteHeaders) throws IOException, CSVStreamException {
		parse(reader, headersValidator, lineConverter, resultConsumer, substituteHeaders, DEFAULT_HEADER_COUNT);
	}

	/**
	 * Stream a CSV file from the given Reader through the header validator, line
	 * checker, and if the line checker succeeds, send the checked/converted line to
	 * the consumer.
	 * 
	 * @param reader
	 *            The {@link Reader} containing the CSV file.
	 * @param headersValidator
	 *            The validator of the header line. Throwing
	 *            IllegalArgumentException or other RuntimeExceptions causes the
	 *            parsing process to short-circuit after parsing the header line,
	 *            with a CSVStreamException being rethrown by this code.
	 * @param lineConverter
	 *            The validator and converter of lines, based on the header line. If
	 *            the lineChecker returns null, the line will not be passed to the
	 *            writer.
	 * @param resultConsumer
	 *            The consumer of the checked lines.
	 * @param substituteHeaders
	 *            A substitute set of headers or null to use the headers from the
	 *            file. If this is null and headerLineCount is set to 0, an
	 *            IllegalArgumentException ill be thrown.
	 * @param headerLineCount
	 *            The number of header lines to expect
	 * @param <T>
	 *            The type of the results that will be created by the lineChecker
	 *            and pushed into the writer {@link Consumer}.
	 * @throws IOException
	 *             If an error occurred accessing the input.
	 * @throws CSVStreamException
	 *             If an error occurred validating the input.
	 */
	public static <T> void parse(final Reader reader, final Consumer<List<String>> headersValidator,
			final BiFunction<List<String>, List<String>, T> lineConverter, final Consumer<T> resultConsumer,
			final List<String> substituteHeaders, int headerLineCount) throws IOException, CSVStreamException {
		parse(reader, headersValidator, lineConverter, resultConsumer, substituteHeaders, Collections.emptyList(),
				headerLineCount);
	}

	/**
	 * Stream a CSV file from the given Reader through the header validator, line
	 * checker, and if the line checker succeeds, send the checked/converted line to
	 * the consumer.
	 * 
	 * @param reader
	 *            The {@link Reader} containing the CSV file.
	 * @param headersValidator
	 *            The validator of the header line. Throwing
	 *            IllegalArgumentException or other RuntimeExceptions causes the
	 *            parsing process to short-circuit after parsing the header line,
	 *            with a CSVStreamException being rethrown by this code.
	 * @param lineConverter
	 *            The validator and converter of lines, based on the header line. If
	 *            the lineChecker returns null, the line will not be passed to the
	 *            writer.
	 * @param resultConsumer
	 *            The consumer of the checked lines.
	 * @param substituteHeaders
	 *            A substitute set of headers or null to use the headers from the
	 *            file. If this is null and headerLineCount is set to 0, an
	 *            IllegalArgumentException ill be thrown.
	 * @param defaultValues
	 *            A list that is either empty, signifying there are no default
	 *            values known, or exactly the same length as each row in the CSV
	 *            file being parsed. If the values for a field are empty/missing,
	 *            and a non-null, non-empty value appears in this list, it will be
	 *            substituted in when calculating the statistics.
	 * @param headerLineCount
	 *            The number of header lines to expect
	 * @param <T>
	 *            The type of the results that will be created by the lineChecker
	 *            and pushed into the writer {@link Consumer}.
	 * @throws IOException
	 *             If an error occurred accessing the input.
	 * @throws CSVStreamException
	 *             If an error occurred validating the input.
	 */
	public static <T> void parse(final Reader reader, final Consumer<List<String>> headersValidator,
			final BiFunction<List<String>, List<String>, T> lineConverter, final Consumer<T> resultConsumer,
			final List<String> substituteHeaders, final List<String> defaultValues, int headerLineCount)
			throws IOException, CSVStreamException {
		if (headerLineCount < 0) {
			throw new IllegalArgumentException("Header line count must be non-negative.");
		}

		if (headerLineCount < 1 && substituteHeaders == null) {
			throw new IllegalArgumentException(
					"If there are no header lines, a substitute set of headers must be defined.");
		}

		List<String> headers = substituteHeaders;
		JsonPointer basePath = JsonPointer.compile("/");
		Map<String, JsonPointer> fieldRelativePaths = new HashMap<>();

		if (headers == null) {
			headers = fieldRelativePaths.keySet().stream().map(v -> v.trim()).map(v -> v.intern())
					.collect(Collectors.toCollection(ArrayList::new));
			Collections.sort(headers, String::compareTo);

			if (!defaultValues.isEmpty()) {
				System.err.println(
						"Substitute headers were not set, but default values were. There is a risk that the default values may not be sorted in the same way as the output values.");
			}
		}

		if (headers != null) {
			try {
				headersValidator.accept(headers);
			} catch (final Exception e) {
				throw new CSVStreamException("Could not verify substituted headers for csv file", e);
			}
		}

		// Default values must either be empty or the exact length
		// that the headers (possibly substituteHeaders) were
		if (!defaultValues.isEmpty() && headers.size() != defaultValues.size()) {
			throw new CSVStreamException(
					"Default values list must have the same number of items as the headers: expected " + headers.size()
							+ ", found " + defaultValues.size() + " headers=" + headers + " defaultValues="
							+ defaultValues);
		}

		List<JsonPointer> fieldRelativePointers = new ArrayList<>(headers.size());

		final Function<List<String>, List<String>> defaultValueReplacer;
		// Trivial non-replacer if there were no default values set
		if (defaultValues.isEmpty()) {
			defaultValueReplacer = l -> l;
		} else {
			defaultValueReplacer = l -> {
				List<String> changedResult = null;
				for (int i = 0; i < l.size(); i++) {
					if (l.get(i).isEmpty() && !defaultValues.get(i).isEmpty()) {
						if (changedResult == null) {
							changedResult = new ArrayList<>(l);
						}
						changedResult.set(i, defaultValues.get(i));
					}
				}
				if (changedResult == null) {
					return l;
				} else {
					return changedResult;
				}
			};
		}

		int lineCount = 0;

		ObjectMapper mapper = new ObjectMapper();

		try (JsonParser parser = mapper.getFactory().createParser(reader);) {
			JsonNode baseNode = mapper.reader().at(basePath).readTree(parser);

			if (!baseNode.isArray()) {
				throw new CSVStreamException("Currently only support base JSONPath's pointing to arrays");
			}

			Iterator<JsonNode> elementIterator = baseNode.elements();
			while (elementIterator.hasNext()) {
				JsonNode nextNode = elementIterator.next();
				List<String> nextLine = new ArrayList<>(fieldRelativePaths.size());
				for (int i = 0; i < fieldRelativePointers.size(); i++) {
					JsonPointer nextField = fieldRelativePointers.get(i);
					// read everything from this START_OBJECT to the matching END_OBJECT
					// and return it as a tree model ObjectNode
					JsonNode node = nextNode.at(nextField);
					if (!node.isValueNode()) {
						throw new CSVStreamException("Field relative pointers must point to value nodes: instead found "
								+ nextField.toString() + " after setting base to " + basePath.toString());
					}
					nextLine.set(i, node.asText());
				}
				// System.out.println("CSVStream.parse: nextLine.size()=" + nextLine.size());
				// System.out.println("CSVStream.parse: nextLine=" + nextLine);
				// System.out.println("CSVStream.parse: schema.getColumnSeparator()=" +
				// schema.getColumnSeparator());
				// System.out.println(
				// "CSVStream.parse: (int)schema.getColumnSeparator()=" + (int)
				// schema.getColumnSeparator());
				if (nextLine.size() != headers.size()) {
					throw new CSVStreamException("Line and header sizes were different: expected " + headers.size()
							+ ", found " + nextLine.size() + " headers=" + headers + " line=" + nextLine);
				}

				final List<String> defaultReplacedLine = defaultValueReplacer.apply(nextLine);

				final T apply = lineConverter.apply(headers, defaultReplacedLine);

				// Line checker returning null indicates that a value was
				// not found, and will not be sent to the consumer.
				if (apply != null) {
					resultConsumer.accept(apply);
				}
				lineCount++;
			}
		} catch (IOException | CSVStreamException e) {
			throw e;
		} catch (Exception e) {
			throw new CSVStreamException(e);
		}
	}

}
