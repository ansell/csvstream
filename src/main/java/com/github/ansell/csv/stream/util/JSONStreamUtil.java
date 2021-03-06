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
package com.github.ansell.csv.stream.util;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON utilities used by CSV and JSON processors.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class JSONStreamUtil {

    /**
     * Private constructor for static-only class
     */
    private JSONStreamUtil() {
    }
    
	private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
	private static final JsonFactory JSON_FACTORY = new JsonFactory(JSON_MAPPER);
	public static JsonNode loadJSON(Path path) throws JsonProcessingException, IOException {
		try (final Reader input = Files.newBufferedReader(path, StandardCharsets.UTF_8);) {
			return loadJSON(input);
		}
	}

	public static JsonNode loadJSON(Reader input) throws JsonProcessingException, IOException {
		return JSON_MAPPER.readTree(input);
	}

	public static String queryJSON(Reader input, String jpath) throws JsonProcessingException, IOException {
		return queryJSON(input, JsonPointer.compile(jpath));
	}

	public static String queryJSON(Reader input, JsonPointer jpath) throws JsonProcessingException, IOException {
		JsonNode root = JSON_MAPPER.readTree(input);
		return queryJSONNode(root, jpath).asText();
	}

	public static JsonNode queryJSONNode(JsonNode input, String jpath) throws JsonProcessingException, IOException {
		return queryJSONNode(input, JsonPointer.compile(jpath));
	}

	public static JsonNode queryJSONNode(JsonNode input, JsonPointer jpath)
			throws JsonProcessingException, IOException {
		return input.at(jpath);
	}

	public static String queryJSONNodeAsText(JsonNode input, String jpath) throws JsonProcessingException, IOException {
		return queryJSONNodeAsText(input, JsonPointer.compile(jpath));
	}

	public static String queryJSONNodeAsText(JsonNode input, JsonPointer jpath)
			throws JsonProcessingException, IOException {
		return queryJSONNode(input, jpath).asText();
	}

	public static void toPrettyPrint(Reader input, Writer output) throws IOException {
		final JsonGenerator jw = JSON_FACTORY.createGenerator(output);
		jw.useDefaultPrettyPrinter();
		JsonParser parser = JSON_FACTORY.createParser(input);
		jw.writeObject(parser.readValueAsTree());
	}

	public static void toPrettyPrint(Map<String, Object> input, Writer output) throws IOException {
		final JsonGenerator jw = JSON_FACTORY.createGenerator(output);
		jw.useDefaultPrettyPrinter();
		jw.writeObject(input);
	}

	public static void toPrettyPrint(JsonNode input, Writer output) throws IOException {
		final JsonGenerator jw = JSON_FACTORY.createGenerator(output);
		jw.useDefaultPrettyPrinter();
		jw.writeObject(input);
	}

	public static String toPrettyPrint(JsonNode input) throws IOException {
		Writer output = new StringWriter();
		final JsonGenerator jw = JSON_FACTORY.createGenerator(output);
		jw.useDefaultPrettyPrinter();
		jw.writeObject(input);
		return output.toString();
	}

}
