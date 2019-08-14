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
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.filter.FilteringParserDelegate;
import com.fasterxml.jackson.core.filter.JsonPointerBasedFilter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Implements streaming of JSON files for parsing using Java-8 Lambda functions
 * such as {@link Consumer} and {@link BiFunction}.
 *
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class JSONStream {

    /**
     * Private constructor for static only class
     */
    private JSONStream() {
    }

    /**
     * Stream a JSON file from the given Reader through the header validator,
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
     * @param basePath
     *            The path to go to before checking the field paths (only
     *            supports a single point of entry at this point in time). Set
     *            to "/" to start at the top of the document. If the basePath
     *            points to an array, each of the array elements are matched
     *            separately with the fieldRelativePaths. If it points to an
     *            object, the object is directly matched to obtain a single
     *            result row. Otherwise an exception is thrown.
     * @param fieldRelativePaths
     *            The relative paths underneath the basePath to select field
     *            values from.
     * @param defaultValues
     *            Default values for fields to use if there are either no, or
     *            empty, values discovered in the document for given fields. The
     *            default values are substituted in before the lineConverter
     *            function is called.
     * @param mapper
     *            The {@link ObjectMapper} to use to parse the JSON document.
     * @param outputHeaders
     *            The header list to use in the output.
     * @param <T>
     *            The type of the results that will be created by the
     *            lineChecker and pushed into the writer {@link Consumer}.
     * @throws IOException
     *             If an error occurred accessing the input.
     * @throws CSVStreamException
     *             If an error occurred validating the input.
     */
    public static <T> void parse(final Reader reader, final Consumer<List<String>> headersValidator,
            final TriFunction<JsonNode, List<String>, List<String>, T> lineConverter,
            final Consumer<T> resultConsumer, final JsonPointer basePath,
            final Map<String, Optional<JsonPointer>> fieldRelativePaths,
            final Map<String, String> defaultValues, final ObjectMapper mapper,
            final List<String> outputHeaders) throws IOException, CSVStreamException {

        if (fieldRelativePaths.isEmpty()) {
            throw new CSVStreamException("No field paths were set for JSONStream.parse");
        }

        // final List<String> headers =
        // fieldRelativePaths.keySet().stream().map(v ->
        // v.trim()).map(v -> v.intern())
        // .collect(Collectors.toCollection(ArrayList::new));
        // Collections.sort(headers, String::compareTo);

        try {
            headersValidator.accept(outputHeaders);
        } catch (final Exception e) {
            throw new CSVStreamException("Could not verify substituted headers for json file", e);
        }

        final List<JsonPointer> fieldRelativePointers = new ArrayList<>(outputHeaders.size());
        for (final String nextHeader : outputHeaders) {
            if (!fieldRelativePaths.containsKey(nextHeader)) {
                throw new CSVStreamException(
                        "No relative JSONPath mapping found for header: " + nextHeader);
            }
            final Optional<JsonPointer> optionalFieldRelativePath = fieldRelativePaths
                    .get(nextHeader);
            if (optionalFieldRelativePath.isPresent()) {
                fieldRelativePointers.add(optionalFieldRelativePath.get());
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
                    if (l.get(i).isEmpty()) {
                        final String nextHeader = outputHeaders.get(i);
                        if (defaultValues.containsKey(nextHeader)) {
                            final String nextDefaultValue = defaultValues.get(nextHeader);
                            if (!nextDefaultValue.isEmpty()) {
                                if (changedResult == null) {
                                    changedResult = new ArrayList<>(l);
                                }
                                changedResult.set(i, nextDefaultValue);
                            }
                        }
                    }
                }
                // If no defaults matched, return the original line instead of
                // copying it to a
                // new array
                if (changedResult == null) {
                    return l;
                } else {
                    return changedResult;
                }
            };
        }

        int lineCount = 0;

        // Parent must not be shown, so we can know whether it is an array or
        // object
        // after a single call to nextToken and to avoid encoding the last part
        // of the
        // base path into the fieldRelativePaths
        final boolean includeParent = false;
        try (JsonParser baseParser = mapper.getFactory().createParser(reader);) {
            final String serialisedBasePath = basePath.toString();
            final JsonParser filteredParser;
            // Only use FilteringParserDelegate if the base path isn't the start
            if (serialisedBasePath.isEmpty() || serialisedBasePath.equals("/")) {
                filteredParser = baseParser;
            } else {
                filteredParser = new FilteringParserDelegate(baseParser,
                        new JsonPointerBasedFilter(basePath), includeParent, false);
            }
            try {
                // Streaming check so that we don't attempt to parse entire
                // large arrays as
                // trees, rather we stream through the objects internally
                // parsing each of the
                // internal objects as separate trees
                // THe FilteringParserDelegate along with the
                // JsonPointerBasedFilter ensures
                // that the first reported token is in the desired location to
                // start finding
                // result records
                // NOTE: The token can be null if the path didn't match
                // anything, in which case,
                // a CSVStreamException will be thrown
                final JsonToken basePathToken = filteredParser.nextToken();
                // If the path points to an array, we iterate over the elements,
                // otherwise
                // process the node as a whole
                if (basePathToken == JsonToken.START_ARRAY) {
                    while (filteredParser.nextToken() == JsonToken.START_OBJECT) {
                        // read everything from this START_OBJECT to the
                        // matching END_OBJECT
                        // and return it as a tree model JsonNode
                        final JsonNode nextNode = mapper.readTree(filteredParser);

                        convertNodeToResult(lineConverter, resultConsumer, basePath,
                                fieldRelativePaths, outputHeaders, fieldRelativePointers,
                                defaultValueReplacer, nextNode);
                        lineCount++;
                    }
                } else if (basePathToken == JsonToken.START_OBJECT) {
                    final JsonNode baseNode = filteredParser.readValueAsTree();

                    if (baseNode == null) {
                        throw new CSVStreamException(
                                "Path did not match anything: path='" + basePath.toString() + "'");
                    }

                    convertNodeToResult(lineConverter, resultConsumer, basePath, fieldRelativePaths,
                            outputHeaders, fieldRelativePointers, defaultValueReplacer, baseNode);
                    lineCount++;
                } else {
                    throw new CSVStreamException(
                            "Base JSONPointer must point to either an Array or an Object: instead found "
                                    + basePathToken + " (path was " + basePath.toString() + ")");
                }
            } finally {
                filteredParser.close();
            }
        } catch (IOException | CSVStreamException e) {
            throw e;
        } catch (final Exception e) {
            throw new CSVStreamException(e);
        }
    }

    public static <T> void convertNodeToResult(
            final TriFunction<JsonNode, List<String>, List<String>, T> lineConverter,
            final Consumer<T> resultConsumer, final JsonPointer basePath,
            final Map<String, Optional<JsonPointer>> fieldRelativePaths, List<String> headers,
            List<JsonPointer> fieldRelativePointers,
            final Function<List<String>, List<String>> defaultValueReplacer, JsonNode nextNode) {
        // System.out.println("JSONStream.parse: headers.size()=" +
        // headers.size());
        // System.out.println("JSONStream.parse: headers=" + headers);
        final int fieldCount = headers.size();
        final List<String> nextLine = initialiseResult(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            final String nextHeader = headers.get(i);
            final Optional<JsonPointer> nextField = fieldRelativePaths.get(nextHeader);
            if (nextField.isPresent()) {
                final JsonNode node = nextNode.at(nextField.get());
                if (node.isValueNode()) {
                    nextLine.set(i, node.asText());
                } else {
                    // throw new CSVStreamException("Field relative pointers
                    // must point to value
                    // nodes: instead found "
                    // + nextField.toString() + " after setting base to " +
                    // basePath.toString());
                }
            }
        }
        // System.out.println("JSONStream.parse: nextLine.size()=" +
        // nextLine.size());
        // System.out.println("JSONStream.parse: nextLine=" + nextLine);
        final List<String> defaultReplacedLine = defaultValueReplacer.apply(nextLine);

        final T apply = lineConverter.apply(nextNode, headers, defaultReplacedLine);

        // Line checker returning null indicates that a value was
        // not found, and will not be sent to the consumer.
        if (apply != null) {
            resultConsumer.accept(apply);
        }
    }

    /**
     * Creates a new mutable {@link List} with size empty Strings in it.
     *
     * @param size
     *            The number of empty strings to be in the result list
     * @return A {@link List} with size empty strings.
     */
    private static List<String> initialiseResult(int size) {
        final List<String> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            result.add("");
        }
        return result;
    }

}
