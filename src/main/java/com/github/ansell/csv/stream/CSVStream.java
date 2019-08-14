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
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType;

/**
 * Implements streaming of CSV files for both parsing and writing using Java-8
 * Lambda functions such as {@link Consumer} and {@link BiFunction}..
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class CSVStream {

    public static final int DEFAULT_HEADER_COUNT = 1;

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
     * @param headersValidator
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
    public static <T> void parse(final InputStream inputStream,
            final Consumer<List<String>> headersValidator,
            final BiFunction<List<String>, List<String>, T> lineConverter,
            final Consumer<T> resultConsumer) throws IOException, CSVStreamException {
        try (final Reader inputStreamReader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8));) {
            parse(inputStreamReader, headersValidator, lineConverter, resultConsumer);
        }
    }

    /**
     * Stream a CSV file from the given Reader through the header validator,
     * line checker, and if the line checker succeeds, send the
     * checked/converted line to the consumer.
     * 
     * @param reader
     *            The {@link Reader} containing the CSV file.
     * @param headersValidator
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
    public static <T> void parse(final Reader reader, final Consumer<List<String>> headersValidator,
            final BiFunction<List<String>, List<String>, T> lineConverter,
            final Consumer<T> resultConsumer) throws IOException, CSVStreamException {
        parse(reader, headersValidator, lineConverter, resultConsumer, null);
    }

    /**
     * Stream a CSV file from the given Reader through the header validator,
     * line checker, and if the line checker succeeds, send the
     * checked/converted line to the consumer.
     * 
     * @param reader
     *            The {@link Reader} containing the CSV file.
     * @param headersValidator
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
     * @param substituteHeaders
     *            A substitute set of headers or null to use the headers from
     *            the file. If this is null the first line of the file will be
     *            used.
     * @param <T>
     *            The type of the results that will be created by the
     *            lineChecker and pushed into the writer {@link Consumer}.
     * @throws IOException
     *             If an error occurred accessing the input.
     * @throws CSVStreamException
     *             If an error occurred validating the input.
     */
    public static <T> void parse(final Reader reader, final Consumer<List<String>> headersValidator,
            final BiFunction<List<String>, List<String>, T> lineConverter,
            final Consumer<T> resultConsumer, final List<String> substituteHeaders)
            throws IOException, CSVStreamException {
        parse(reader, headersValidator, lineConverter, resultConsumer, substituteHeaders,
                DEFAULT_HEADER_COUNT);
    }

    /**
     * Stream a CSV file from the given Reader through the header validator,
     * line checker, and if the line checker succeeds, send the
     * checked/converted line to the consumer.
     * 
     * @param reader
     *            The {@link Reader} containing the CSV file.
     * @param headersValidator
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
     * @param substituteHeaders
     *            A substitute set of headers or null to use the headers from
     *            the file. If this is null and headerLineCount is set to 0, an
     *            IllegalArgumentException ill be thrown.
     * @param headerLineCount
     *            The number of header lines to expect
     * @param <T>
     *            The type of the results that will be created by the
     *            lineChecker and pushed into the writer {@link Consumer}.
     * @throws IOException
     *             If an error occurred accessing the input.
     * @throws CSVStreamException
     *             If an error occurred validating the input.
     */
    public static <T> void parse(final Reader reader, final Consumer<List<String>> headersValidator,
            final BiFunction<List<String>, List<String>, T> lineConverter,
            final Consumer<T> resultConsumer, final List<String> substituteHeaders,
            int headerLineCount) throws IOException, CSVStreamException {
        final CsvMapper mapper = defaultMapper();
        final CsvSchema schema = defaultSchema();

        parse(reader, headersValidator, lineConverter, resultConsumer, substituteHeaders,
                headerLineCount, mapper, schema);
    }

    /**
     * Stream a CSV file from the given Reader through the header validator,
     * line checker, and if the line checker succeeds, send the
     * checked/converted line to the consumer.
     * 
     * @param reader
     *            The {@link Reader} containing the CSV file.
     * @param headersValidator
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
     * @param substituteHeaders
     *            A substitute set of headers or null to use the headers from
     *            the file. If this is null and headerLineCount is set to 0, an
     *            IllegalArgumentException ill be thrown.
     * @param headerLineCount
     *            The number of header lines to expect
     * @param mapper
     *            The {@link CsvMapper} to use to parse the CSV document.
     * @param schema
     *            The {@link CsvSchema} to use when parsing the CSV document.
     * @param <T>
     *            The type of the results that will be created by the
     *            lineChecker and pushed into the writer {@link Consumer}.
     * @throws IOException
     *             If an error occurred accessing the input.
     * @throws CSVStreamException
     *             If an error occurred validating the input.
     */
    public static <T> void parse(final Reader reader, final Consumer<List<String>> headersValidator,
            final BiFunction<List<String>, List<String>, T> lineConverter,
            final Consumer<T> resultConsumer, final List<String> substituteHeaders,
            int headerLineCount, CsvMapper mapper, CsvSchema schema)
            throws IOException, CSVStreamException {
        parse(reader, headersValidator, lineConverter, resultConsumer, substituteHeaders,
                Collections.emptyList(), headerLineCount, mapper, schema);
    }

    /**
     * Stream a CSV file from the given Reader through the header validator,
     * line checker, and if the line checker succeeds, send the
     * checked/converted line to the consumer.
     * 
     * @param reader
     *            The {@link Reader} containing the CSV file.
     * @param headersValidator
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
     * @param substituteHeaders
     *            A substitute set of headers or null to use the headers from
     *            the file. If this is null and headerLineCount is set to 0, an
     *            IllegalArgumentException ill be thrown.
     * @param defaultValues
     *            A list that is either empty, signifying there are no default
     *            values known, or exactly the same length as each row in the
     *            CSV file being parsed. If the values for a field are
     *            empty/missing, and a non-null, non-empty value appears in this
     *            list, it will be substituted in when calculating the
     *            statistics. The default values are substituted in before the
     *            lineConverter function is called.
     * @param headerLineCount
     *            The number of header lines to expect
     * @param mapper
     *            The {@link CsvMapper} to use to parse the CSV document.
     * @param schema
     *            The {@link CsvSchema} to use when parsing the CSV document.
     * @param <T>
     *            The type of the results that will be created by the
     *            lineChecker and pushed into the writer {@link Consumer}.
     * @throws IOException
     *             If an error occurred accessing the input.
     * @throws CSVStreamException
     *             If an error occurred validating the input.
     */
    public static <T> void parse(final Reader reader, final Consumer<List<String>> headersValidator,
            final BiFunction<List<String>, List<String>, T> lineConverter,
            final Consumer<T> resultConsumer, final List<String> substituteHeaders,
            final List<String> defaultValues, int headerLineCount, CsvMapper mapper,
            CsvSchema schema) throws IOException, CSVStreamException {
        if (headerLineCount < 0) {
            throw new IllegalArgumentException("Header line count must be non-negative.");
        }

        if (headerLineCount < 1 && substituteHeaders == null) {
            throw new IllegalArgumentException(
                    "If there are no header lines, a substitute set of headers must be defined.");
        }

        List<String> headers = substituteHeaders;

        if (headers != null) {
            try {
                headers = headers.stream().map(String::trim).map(String::intern)
                        .collect(Collectors.toList());
                headersValidator.accept(headers);
            } catch (final Exception e) {
                throw new CSVStreamException("Could not verify substituted headers for csv file",
                        e);
            }
        }

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
        try (final MappingIterator<List<String>> it = mapper.readerFor(List.class).with(schema)
                .readValues(reader);) {
            while (it.hasNext()) {
                List<String> nextLine = it.next();
                // System.out.println("CSVStream.parse: nextLine.size()=" +
                // nextLine.size());
                // System.out.println("CSVStream.parse: nextLine=" + nextLine);
                // System.out.println("CSVStream.parse:
                // schema.getColumnSeparator()=" +
                // schema.getColumnSeparator());
                // System.out.println(
                // "CSVStream.parse: (int)schema.getColumnSeparator()=" + (int)
                // schema.getColumnSeparator());
                if (headers == null) {
                    headers = nextLine.stream().map(String::trim).map(String::intern)
                            .collect(Collectors.toList());
                    try {
                        headersValidator.accept(headers);
                    } catch (final Exception e) {
                        throw new CSVStreamException("Could not verify headers for csv file", e);
                    }
                    // Default values must either be empty or the exact length
                    // that the headers (possibly substituteHeaders) were
                    if (!defaultValues.isEmpty() && headers.size() != defaultValues.size()) {
                        throw new CSVStreamException(
                                "Default values list must have the same number of items as the headers: expected "
                                        + headers.size() + ", found " + defaultValues.size()
                                        + " headers=" + headers + " defaultValues="
                                        + defaultValues);
                    }
                } else if (lineCount >= headerLineCount) {
                    if (nextLine.size() != headers.size()) {
                        throw new CSVStreamException(
                                "Line and header sizes were different: expected " + headers.size()
                                        + ", found " + nextLine.size() + " headers=" + headers
                                        + " line=" + nextLine);
                    }

                    final List<String> defaultReplacedLine = defaultValueReplacer.apply(nextLine);

                    final T apply = lineConverter.apply(headers, defaultReplacedLine);

                    // Line checker returning null indicates that a value was
                    // not found, and will not be sent to the consumer.
                    if (apply != null) {
                        resultConsumer.accept(apply);
                    }
                } else {
                    // System.out.println("CSVStream.parse: skipping header line
                    // number " +
                    // (lineCount + 1) + " out of "
                    // + headerLineCount + " : " + nextLine);
                }
                lineCount++;
            }
        } catch (IOException | CSVStreamException e) {
            throw e;
        } catch (Exception e) {
            throw new CSVStreamException(e);
        }

        if (headers == null) {
            throw new CSVStreamException("CSV file did not contain a valid header line");
        }
    }

    /**
     * Writes objects from the given {@link Stream} to the given {@link Writer}
     * in CSV format, converting them to a {@link List} of String's using the
     * given {@link BiFunction}.
     * 
     * @param writer
     *            The Writer that will receive the CSV file.
     * @param objects
     *            The Stream of objects to be written
     * @param headers
     *            The headers to use for the resulting CSV file.
     * @param objectConverter
     *            The function to convert an individual object to a line in the
     *            resulting CSV file, represented as a List of String's.
     * @param <T>
     *            The type of the objects to be converted.
     * @throws IOException
     *             If an error occurred accessing the output stream.
     * @throws CSVStreamException
     *             If an error occurred converting or serialising the objects.
     */
    public static <T> void write(final Writer writer, final Stream<T> objects,
            final List<String> headers,
            final BiFunction<List<String>, T, List<String>> objectConverter)
            throws IOException, CSVStreamException {
        write(writer, objects, buildSchema(headers), objectConverter);
    }

    /**
     * Writes objects from the given {@link Stream} to the given {@link Writer}
     * in CSV format, converting them to a {@link List} of String's using the
     * given {@link BiFunction}.
     * 
     * @param writer
     *            The Writer that will receive the CSV file.
     * @param objects
     *            The Stream of objects to be written
     * @param schema
     *            The {@link CsvSchema} to use for the resulting CSV file.
     * @param objectConverter
     *            The function to convert an individual object to a line in the
     *            resulting CSV file, represented as a List of String's.
     * @param <T>
     *            The type of the objects to be converted.
     * @throws IOException
     *             If an error occurred accessing the output stream.
     * @throws CSVStreamException
     *             If an error occurred converting or serialising the objects.
     */
    public static <T> void write(final Writer writer, final Stream<T> objects,
            final CsvSchema schema, final BiFunction<List<String>, T, List<String>> objectConverter)
            throws IOException, CSVStreamException {
        List<String> headers = new ArrayList<>();
        schema.iterator().forEachRemaining(c -> headers.add(c.getName()));
        try (SequenceWriter csvWriter = newCSVWriter(writer, headers);) {
            objects.forEachOrdered(o -> {
                try {
                    csvWriter.write(objectConverter.apply(headers, o));
                } catch (Exception e) {
                    throw new CSVStreamException("Could not write object out", e);
                }
            });
        }
    }

    /**
     * Returns a Jackson {@link SequenceWriter} which will write CSV lines to
     * the given {@link OutputStream} using the headers provided.
     * 
     * @param outputStream
     *            The writer which will receive the CSV file.
     * @param headers
     *            The column headers that will be used by the returned Jackson
     *            {@link SequenceWriter}.
     * @return A Jackson {@link SequenceWriter} that can have
     *         {@link SequenceWriter#write(Object)} called on it to emit CSV
     *         lines to the given {@link OutputStream}.
     * @throws IOException
     *             If there is a problem writing the CSV header line to the
     *             {@link OutputStream}.
     */
    public static SequenceWriter newCSVWriter(final OutputStream outputStream, List<String> headers)
            throws IOException {
        return newCSVWriter(outputStream, buildSchema(headers));
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
    public static SequenceWriter newCSVWriter(final OutputStream outputStream, CsvSchema schema)
            throws IOException {
        return defaultMapper().writerWithDefaultPrettyPrinter().with(schema).forType(List.class)
                .writeValues(outputStream);
    }

    /**
     * Returns a Jackson {@link SequenceWriter} which will write CSV lines to
     * the given {@link Writer} using the headers provided.
     * 
     * @param writer
     *            The writer which will receive the CSV file.
     * @param headers
     *            The column headers that will be used by the returned Jackson
     *            {@link SequenceWriter}.
     * @return A Jackson {@link SequenceWriter} that can have
     *         {@link SequenceWriter#write(Object)} called on it to emit CSV
     *         lines to the given {@link Writer}.
     * @throws IOException
     *             If there is a problem writing the CSV header line to the
     *             {@link Writer}.
     */
    public static SequenceWriter newCSVWriter(final Writer writer, List<String> headers)
            throws IOException {
        return newCSVWriter(writer, buildSchema(headers));
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
    public static SequenceWriter newCSVWriter(final Writer writer, CsvSchema schema)
            throws IOException {
        return defaultMapper().writerWithDefaultPrettyPrinter().with(schema).forType(List.class)
                .writeValues(writer);
    }

    /**
     * Build a {@link CsvSchema} object using the given headers.
     * 
     * @param headers
     *            The list of strings in the header.
     * @return A {@link CsvSchema} object including the given header items.
     */
    public static CsvSchema buildSchema(List<String> headers) {
        return buildSchema(headers, true);
    }

    /**
     * Build a {@link CsvSchema} object using the given headers.
     * 
     * @param headers
     *            The list of strings in the header.
     * @param useHeader
     *            Set to false to avoid writing the header line, which is
     *            necessary if appending to an existing file.
     * @return A {@link CsvSchema} object including the given header items.
     */
    public static CsvSchema buildSchema(List<String> headers, boolean useHeader) {
        return CsvSchema.builder().addColumns(headers, ColumnType.STRING).setUseHeader(useHeader)
                .build();
    }

    /**
     * Returns a {@link CsvMapper} that contains the default settings used by
     * csvstream.
     * 
     * @return A new {@link CsvMapper} setup to match the defaults used by
     *         csvstream
     */
    public static CsvMapper defaultMapper() {
        final CsvMapper mapper = new CsvMapper();
        mapper.enable(CsvParser.Feature.TRIM_SPACES);
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
        mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
        return mapper;
    }

    /**
     * Returns a {@link CsvSchema} that contains the default settings used by
     * csvstream.
     * 
     * @return A new {@link CsvSchema} setup to match the defaults used by
     *         csvstream
     */
    public static CsvSchema defaultSchema() {
        return CsvSchema.emptySchema();
    }

}
