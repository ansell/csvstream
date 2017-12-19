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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonToken;
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

	private ObjectMapper mapper = new ObjectMapper();
	
	@Test
	public void testParseTopLevelObjectWithArrayPath() throws Exception {

		String testString = "{ \"base\": [\n" + "       {\n" + "        \"name\":\"Alice\",\n"
				+ "        \"phone\": [{\n" + "            \"home\": \"1234567890\",\n"
				+ "            \"mobile\": \"0001112223\"\n" + "        }]\n" + "    },\n" + "    {\n"
				+ "        \"name\":\"Bob\",\n" + "        \"phone\": [{\n" + "            \"home\": \"3456789012\",\n"
				+ "            \"mobile\": \"4445556677\"\n" + "        }]\n" + "    }\n" + "] }";

		TriFunction<JsonNode, List<String>, List<String>, List<String>> lineConverter = (node, header, line) -> {
			System.out.println(header);
			assertEquals(header.size(), 3);
			assertEquals(line.size(), 3);
			assertEquals("homePhone", header.get(0));
			assertEquals("mobilePhone", header.get(1));
			assertEquals("name", header.get(2));
			return line;
		};
		Consumer<List<String>> resultConsumer = l -> {
			System.out.println(l);
			assertEquals(l.size(), 3);
			if (l.get(2).equals("Alice")) {
				assertEquals(l.get(0), "1234567890");
				assertEquals(l.get(1), "0001112223");
			} else if (l.get(2).equals("Bob")) {
				assertEquals(l.get(0), "3456789012");
				assertEquals(l.get(1), "4445556677");
			} else {
				fail("Found unrecognised name field value: " + l.get(2));
			}
		};
		JsonPointer basePath = JsonPointer.compile("/base");
		Map<String, Optional<JsonPointer>> fieldRelativePaths = new LinkedHashMap<>();
		fieldRelativePaths.put("name", Optional.of(JsonPointer.compile("/name")));
		fieldRelativePaths.put("homePhone", Optional.of(JsonPointer.compile("/phone/0/home")));
		fieldRelativePaths.put("mobilePhone", Optional.of(JsonPointer.compile("/phone/0/mobile")));

		Map<String, String> defaultValues = Collections.emptyMap();

		System.out.println("JSONStreamUtil.queryJSON:");
		System.out.println(JSONStreamUtil.queryJSON(new StringReader(testString), basePath));

		System.out.println("JSONStream.parse:");
		JSONStream.parse(new StringReader(testString), h -> {
		}, lineConverter, resultConsumer, basePath, fieldRelativePaths, defaultValues, mapper);

	}

	@Test
	public void testParseTopLevelObjectWithObjectPath() throws Exception {

		String testString = "{ \"base\": \n" + "       {\n" + "        \"name\":\"Alice\",\n"
				+ "        \"phone\": [{\n" + "            \"home\": \"1234567890\",\n"
				+ "            \"mobile\": \"0001112223\"\n" + "        }]\n" + "    } " + "}";

		TriFunction<JsonNode, List<String>, List<String>, List<String>> lineConverter = (node, header, line) -> {
			System.out.println(header);
			assertEquals(header.size(), 3);
			assertEquals(line.size(), 3);
			assertEquals("homePhone", header.get(0));
			assertEquals("mobilePhone", header.get(1));
			assertEquals("name", header.get(2));
			return line;
		};
		Consumer<List<String>> resultConsumer = l -> {
			System.out.println(l);
			assertEquals(l.size(), 3);
			if (l.get(2).equals("Alice")) {
				assertEquals(l.get(0), "1234567890");
				assertEquals(l.get(1), "0001112223");
			} else {
				fail("Found unrecognised name field value: " + l.get(2));
			}
		};
		JsonPointer basePath = JsonPointer.compile("/base");
		Map<String, Optional<JsonPointer>> fieldRelativePaths = new LinkedHashMap<>();
		fieldRelativePaths.put("name", Optional.of(JsonPointer.compile("/name")));
		fieldRelativePaths.put("homePhone", Optional.of(JsonPointer.compile("/phone/0/home")));
		fieldRelativePaths.put("mobilePhone", Optional.of(JsonPointer.compile("/phone/0/mobile")));

		Map<String, String> defaultValues = Collections.emptyMap();

		System.out.println("JSONStreamUtil.queryJSON:");
		System.out.println(JSONStreamUtil.queryJSON(new StringReader(testString), basePath));

		System.out.println("JSONStream.parse:");
		JSONStream.parse(new StringReader(testString), h -> {
		}, lineConverter, resultConsumer, basePath, fieldRelativePaths, defaultValues, mapper);

	}

	@Ignore("Broken")
	@Test
	public void testParseNoTopLevelObject() throws Exception {

		String testString = "\"base\": [\n" + "       {\n" + "        \"name\":\"Alice\",\n" + "        \"phone\": [{\n"
				+ "            \"home\": \"1234567890\",\n" + "            \"mobile\": \"0001112223\"\n"
				+ "        }]\n" + "    },\n" + "    {\n" + "        \"name\":\"Bob\",\n" + "        \"phone\": [{\n"
				+ "            \"home\": \"3456789012\",\n" + "            \"mobile\": \"4445556677\"\n"
				+ "        }]\n" + "    }\n" + "]";

		TriFunction<JsonNode, List<String>, List<String>, List<String>> lineConverter = (node, header, line) -> line;
		Consumer<List<String>> resultConsumer = l -> {
		};
		JsonPointer basePath = JsonPointer.compile("/base/1");
		Map<String, Optional<JsonPointer>> fieldRelativePaths = new LinkedHashMap<>();
		Map<String, String> defaultValues = Collections.emptyMap();

		System.out.println("JSONStreamUtil.queryJSON:");
		System.out.println(JSONStreamUtil.queryJSON(new StringReader(testString), basePath));

		System.out.println("JSONStream.parse:");
		JSONStream.parse(new StringReader(testString), h -> {
		}, lineConverter, resultConsumer, basePath, fieldRelativePaths, defaultValues, mapper);

	}

	@Ignore("Broken")
	@Test
	public void testParseNoTopLevelObjectOrArrays() throws Exception {

		String testString = "\"base\": \n" + "       {\n" + "        \"name\":\"Alice\",\n" + "        \"phone\": [{\n"
				+ "            \"home\": \"1234567890\",\n" + "            \"mobile\": \"0001112223\"\n"
				+ "        }]\n" + "    }\n";

		TriFunction<JsonNode, List<String>, List<String>, List<String>> lineConverter = (node, header, line) -> line;
		Consumer<List<String>> resultConsumer = l -> {
		};
		JsonPointer basePath = JsonPointer.compile("/base/name");
		Map<String, Optional<JsonPointer>> fieldRelativePaths = new LinkedHashMap<>();
		Map<String, String> defaultValues = Collections.emptyMap();

		System.out.println("JSONStreamUtil.queryJSON:");
		System.out.println(JSONStreamUtil.queryJSON(new StringReader(testString), basePath));

		System.out.println("JSONStream.parse:");
		JSONStream.parse(new StringReader(testString), h -> {
		}, lineConverter, resultConsumer, basePath, fieldRelativePaths, defaultValues, mapper);

	}

	@Test
	public void testJsonPointerBasedFilterNoArray() throws Exception {
		ObjectMapper JSON_MAPPER = new ObjectMapper();
		JsonFactory JSON_FACTORY = new JsonFactory(JSON_MAPPER);

		String testString = "{ \"base\": \n" + "       {\n" + "        \"name\":\"Alice\",\n"
				+ "        \"phone\": [{\n" + "            \"home\": \"1234567890\",\n"
				+ "            \"mobile\": \"0001112223\"\n" + "        }]\n" + "    } }\n";

		Reader input = new StringReader(testString);
		JsonParser p0 = JSON_FACTORY.createParser(input);
		String pathExpr = "/base";
		boolean includeParent = false;
		JsonParser p = new FilteringParserDelegate(p0, new JsonPointerBasedFilter(pathExpr), includeParent, false);
		JsonNode readValueAsTree = p.readValueAsTree();
		System.out.println("FilteringParserDelegate + JsonPointerBasedFilter no array:");
		System.out.println(JSONStreamUtil.toPrettyPrint(readValueAsTree));
	}

	@Test
	public void testJsonPointerBasedFilterWithArray() throws Exception {
		ObjectMapper JSON_MAPPER = new ObjectMapper();
		JsonFactory JSON_FACTORY = new JsonFactory(JSON_MAPPER);

		String testString = "{ \"base\": [ \n" + "       {\n" + "        \"name\":\"Alice\",\n"
				+ "        \"phone\": [{\n" + "            \"home\": \"1234567890\",\n"
				+ "            \"mobile\": \"0001112223\"\n" + "        }]\n" + "    } ] }\n";

		Reader input = new StringReader(testString);
		JsonParser p0 = JSON_FACTORY.createParser(input);
		String pathExpr = "/base/0";
		boolean includeParent = false;
		JsonParser p = new FilteringParserDelegate(p0, new JsonPointerBasedFilter(pathExpr), includeParent, false);
		JsonNode readValueAsTree = p.readValueAsTree();
		System.out.println("FilteringParserDelegate + JsonPointerBasedFilter with array:");
		System.out.println(JSONStreamUtil.toPrettyPrint(readValueAsTree));
	}

	@Test
	public void testJsonPointerBasedFilterWithArrayFurtherAnalysis() throws Exception {
		ObjectMapper JSON_MAPPER = new ObjectMapper();
		JsonFactory JSON_FACTORY = new JsonFactory(JSON_MAPPER);

		String testString = "{ \"base\": [ \n" + "       {\n" + "        \"name\":\"Alice\",\n"
				+ "        \"phone\": [{\n" + "            \"home\": \"1234567890\",\n"
				+ "            \"mobile\": \"0001112223\"\n" + "        }]\n" + "    } ] }\n";

		Reader input = new StringReader(testString);
		JsonParser p0 = JSON_FACTORY.createParser(input);
		String pathExpr = "/base/0";
		boolean includeParent = false;
		JsonParser p = new FilteringParserDelegate(p0, new JsonPointerBasedFilter(pathExpr), includeParent, false);
		JsonNode readValueAsTree = p.readValueAsTree();
		System.out.println("FilteringParserDelegate + JsonPointerBasedFilter with array:");
		System.out.println(JSONStreamUtil.toPrettyPrint(readValueAsTree));
		JsonNode homePhoneNumber = JSONStreamUtil.queryJSONNode(readValueAsTree, "/phone/0/home");
		System.out.println("home phone:");
		String homePhoneNumberString = JSONStreamUtil.toPrettyPrint(homePhoneNumber);
		System.out.println(homePhoneNumberString);
		assertEquals("\"1234567890\"", homePhoneNumberString);
	}

	/**
	 * Test to verify that code in StackOverflow answer derived from this works.
	 * 
	 * @link <a href="https://stackoverflow.com/a/47804189/638674">StackOverflow
	 *       answer for 'Use Jackson To Stream Parse an Array of Json Objects'</a>
	 * @throws Exception
	 */
	@Ignore("Example code only")
	@Test
	public void testStackOverflowAnswer() throws Exception {
		Path sourceFile = Paths.get("/path/to/my/file.json");
		// Point the basePath to a starting point in the file
		JsonPointer basePath = JsonPointer.compile("/");
		ObjectMapper mapper = new ObjectMapper();
		try (InputStream inputSource = Files.newInputStream(sourceFile);
				JsonParser baseParser = mapper.getFactory().createParser(inputSource);
				JsonParser filteredParser = new FilteringParserDelegate(baseParser,
						new JsonPointerBasedFilter(basePath), false, false);) {
			// Call nextToken once to initialize the filteredParser
			JsonToken basePathToken = filteredParser.nextToken();
			if (basePathToken != JsonToken.START_ARRAY) {
				throw new IllegalStateException("Base path did not point to an array: found " + basePathToken);
			}
			while (filteredParser.nextToken() == JsonToken.START_OBJECT) {
				// Parse each object inside of the array into a separate tree model
				// to keep a fixed memory footprint when parsing files
				// larger than the available memory
				JsonNode nextNode = mapper.readTree(filteredParser);
				// Consume/process the node for example:
				JsonPointer fieldRelativePath = JsonPointer.compile("/test1");
				JsonNode valueNode = nextNode.at(fieldRelativePath);
				if (!valueNode.isValueNode()) {
					throw new IllegalStateException("Did not find value " + fieldRelativePath.toString()
							+ " after setting base to " + basePath.toString());
				}
				System.out.println(valueNode.asText());
			}
		}
	}
}
