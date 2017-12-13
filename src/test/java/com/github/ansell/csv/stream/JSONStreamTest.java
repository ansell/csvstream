/*
 * Copyright (c) 2017, Peter Ansell
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

import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.filter.FilteringParserDelegate;
import com.fasterxml.jackson.core.filter.JsonPointerBasedFilter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ansell.csv.stream.util.JSONStreamUtil;

/**
 * Tests for {@link JSONStream}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class JSONStreamTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testParseTopLevelObject() throws Exception {
		
		String testString = "{ \"base\": [\n" + 
				"       {\n" + 
				"        \"name\":\"Alice\",\n" + 
				"        \"phone\": [{\n" + 
				"            \"home\": \"1234567890\",\n" + 
				"            \"mobile\": \"0001112223\"\n" + 
				"        }]\n" + 
				"    },\n" + 
				"    {\n" + 
				"        \"name\":\"Bob\",\n" + 
				"        \"phone\": [{\n" + 
				"            \"home\": \"3456789012\",\n" + 
				"            \"mobile\": \"4445556677\"\n" + 
				"        }]\n" + 
				"    }\n" + 
				"] }";
		
		
		BiFunction<List<String>, List<String>, List<String>> lineConverter = (h, l) -> l;
		Consumer<List<String>> resultConsumer = l -> {
		};
		JsonPointer basePath = JsonPointer.compile("/base");
		Map<String, JsonPointer> fieldRelativePaths = new HashMap<>();
		Map<String, String> defaultValues = Collections.emptyMap();

		System.out.println("JSONStreamUtil.queryJSON:");
		System.out.println(JSONStreamUtil.queryJSON(new StringReader(testString), basePath));
		
		System.out.println("JSONStream.parse:");
		JSONStream.parse(new StringReader(testString), h -> {
		}, lineConverter, resultConsumer, basePath, fieldRelativePaths, defaultValues);

	}

	@Test
	public void testParseNoTopLevelObject() throws Exception {
		
		String testString = "\"base\": [\n" + 
				"       {\n" + 
				"        \"name\":\"Alice\",\n" + 
				"        \"phone\": [{\n" + 
				"            \"home\": \"1234567890\",\n" + 
				"            \"mobile\": \"0001112223\"\n" + 
				"        }]\n" + 
				"    },\n" + 
				"    {\n" + 
				"        \"name\":\"Bob\",\n" + 
				"        \"phone\": [{\n" + 
				"            \"home\": \"3456789012\",\n" + 
				"            \"mobile\": \"4445556677\"\n" + 
				"        }]\n" + 
				"    }\n" + 
				"]";
		
		
		BiFunction<List<String>, List<String>, List<String>> lineConverter = (h, l) -> l;
		Consumer<List<String>> resultConsumer = l -> {
		};
		JsonPointer basePath = JsonPointer.compile("/base/1");
		Map<String, JsonPointer> fieldRelativePaths = new HashMap<>();
		Map<String, String> defaultValues = Collections.emptyMap();

		System.out.println("JSONStreamUtil.queryJSON:");
		System.out.println(JSONStreamUtil.queryJSON(new StringReader(testString), basePath));
		
		System.out.println("JSONStream.parse:");
		JSONStream.parse(new StringReader(testString), h -> {
		}, lineConverter, resultConsumer, basePath, fieldRelativePaths, defaultValues);

	}

	@Test
	public void testParseNoTopLevelObjectOrArrays() throws Exception {
		
		String testString = "\"base\": \n" + 
				"       {\n" + 
				"        \"name\":\"Alice\",\n" + 
				"        \"phone\": [{\n" + 
				"            \"home\": \"1234567890\",\n" + 
				"            \"mobile\": \"0001112223\"\n" + 
				"        }]\n" + 
				"    }\n";
		
		
		BiFunction<List<String>, List<String>, List<String>> lineConverter = (h, l) -> l;
		Consumer<List<String>> resultConsumer = l -> {
		};
		JsonPointer basePath = JsonPointer.compile("/");
		Map<String, JsonPointer> fieldRelativePaths = new HashMap<>();
		Map<String, String> defaultValues = Collections.emptyMap();

		System.out.println("JSONStreamUtil.queryJSON:");
		System.out.println(JSONStreamUtil.queryJSON(new StringReader(testString), basePath));
		
		System.out.println("JSONStream.parse:");
		JSONStream.parse(new StringReader(testString), h -> {
		}, lineConverter, resultConsumer, basePath, fieldRelativePaths, defaultValues);

	}
	
	@Test
	public void testJsonPointerBasedFilter() throws Exception {
		ObjectMapper JSON_MAPPER = new ObjectMapper();
		JsonFactory JSON_FACTORY = new JsonFactory(JSON_MAPPER);
		
		String testString = "{ \"base\": \n" + 
				"       {\n" + 
				"        \"name\":\"Alice\",\n" + 
				"        \"phone\": [{\n" + 
				"            \"home\": \"1234567890\",\n" + 
				"            \"mobile\": \"0001112223\"\n" + 
				"        }]\n" + 
				"    } }\n";
		
        Reader input = new StringReader(testString);
		JsonParser p0 = JSON_FACTORY.createParser(input);
        String pathExpr = "/base";
		boolean includeParent = false;
		JsonParser p = new FilteringParserDelegate(p0,
                new JsonPointerBasedFilter(pathExpr),
                includeParent, false);
		JsonNode readValueAsTree = p.readValueAsTree();
		System.out.println(JSONStreamUtil.toPrettyPrint(readValueAsTree));
	}

}
