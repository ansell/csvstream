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

import static org.junit.Assert.*;

import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class JSONStreamUtilTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.stream.util.JSONStreamUtil#toPrettyPrint(java.io.Reader, java.io.Writer)}
	 * .
	 */
	@Test
	public final void testToPrettyPrint() throws Exception {
		StringWriter output = new StringWriter();
		StringReader input = new StringReader("{\"test\":\"something\"}");
		JSONStreamUtil.toPrettyPrint(input, output);
		System.out.println(output.toString());
		assertTrue(output.toString().contains("\"test\" : \"something\""));
	}

	@Ignore("Requires offline resource")
	@Test
	public final void testToPrettyPrintLarge() throws Exception {
		try (Writer output = Files.newBufferedWriter(Paths.get("/tmp/test-pretty.json"), StandardCharsets.UTF_8);
				Reader input = Files.newBufferedReader(Paths.get("/tmp/test.json"), StandardCharsets.UTF_8);) {
			JSONStreamUtil.toPrettyPrint(input, output);
		}
	}

	@Test
	public final void testPrettyPrintNode() throws Exception {
		StringReader input = new StringReader("{\"owner\": {\"avatar_url\": \"https://avatar.githubusercontent.com/\"}}");
		StringWriter output = new StringWriter();
		JsonNode jsonNode = JSONStreamUtil.loadJSON(input);
		JSONStreamUtil.toPrettyPrint(jsonNode, output);

		System.out.println(output.toString());

		String avatar = JSONStreamUtil.queryJSON(new StringReader(output.toString()), "/owner/avatar_url");

		assertTrue(avatar, avatar.contains(".githubusercontent.com/"));
		assertTrue(avatar, avatar.startsWith("https://avatar"));

		// JSONUtil.queryJSON(
		// "http://bie.ala.org.au/search.json?q=Banksia+occidentalis",
		// "/searchResults/results/0/guid");
	}

	@Test
	public final void testQuery() throws Exception {
		Path testFile = tempDir.newFile().toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csv/stream/util/ala-test.json"), testFile,
				StandardCopyOption.REPLACE_EXISTING);
		try (Reader reader = Files.newBufferedReader(testFile);) {
			JSONStreamUtil.toPrettyPrint(reader, new OutputStreamWriter(System.out));
		}
		System.out.println("");
		try (Reader reader = Files.newBufferedReader(testFile);) {
			System.out.println(JSONStreamUtil.queryJSON(reader, JsonPointer.compile("/searchResults/results/0/guid")));
		}
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.stream.util.JSONStreamUtil#queryJSON(java.io.Reader, JsonPointer)}
	 * .
	 */
	@Test
	public final void testQueryJSON() throws Exception {
		StringReader input = new StringReader("{ \"test\": \"something\"}");
		JsonPointer jpath = JsonPointer.compile("/test");
		String result = JSONStreamUtil.queryJSON(input, jpath);
		System.out.println(result);
		assertEquals("something", result);

		StringReader input2 = new StringReader("{ \"test\": \"something\"}");
		String result2 = JSONStreamUtil.queryJSON(input2, "/test");
		System.out.println(result2);
		assertEquals("something", result2);
	}

	@Test
	public final void testLoadJSONNullProperty() throws Exception {
		Path testNullFile = tempDir.newFolder("jsonutiltest").toPath().resolve("test-null.json");
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csv/stream/util/test-null.json"), testNullFile);
		JsonNode rootNode = JSONStreamUtil.loadJSON(testNullFile);

		JsonNode searchResultsNode = rootNode.get("searchResults");

		JsonNode pageSizeNode = searchResultsNode.get("pageSize");

		JsonNode startIndexNode = searchResultsNode.get("startIndex");

		System.out.println("pageSize=" + pageSizeNode.toString());
		System.out.println("startIndex=" + startIndexNode.toString());
	}
}
