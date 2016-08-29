/**
 * 
 */
package com.github.ansell.csv.stream;

import static org.junit.Assert.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

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
	 * Verifies that a utility class is well defined.
	 * 
	 * @param clazz
	 *            utility class to verify.
	 * @see <a href="http://stackoverflow.com/a/10872497/638674">Source at
	 *      StackOverflow</a>
	 */
	public static void assertUtilityClassWellDefined(final Class<?> clazz)
			throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
		assertTrue("class must be final", Modifier.isFinal(clazz.getModifiers()));
		assertEquals("There must be only one constructor", 1, clazz.getDeclaredConstructors().length);
		final Constructor<?> constructor = clazz.getDeclaredConstructor();
		if (constructor.isAccessible() || !Modifier.isPrivate(constructor.getModifiers())) {
			fail("constructor is not private");
		}
		constructor.setAccessible(true);
		constructor.newInstance();
		constructor.setAccessible(false);
		for (final Method method : clazz.getMethods()) {
			if (!Modifier.isStatic(method.getModifiers()) && method.getDeclaringClass().equals(clazz)) {
				fail("there exists a non-static method:" + method);
			}
		}
	}

	/**
	 * Test method for {@link CSVStream} private constructor.
	 */
	@Test
	public final void testCSVStreamConstructorPrivate() throws Exception {
		assertUtilityClassWellDefined(CSVStream.class);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.stream.CSVStreamException#CSVStreamException()}.
	 */
	@Test
	public final void testCSVStreamException() {
		thrown.expect(CSVStreamException.class);
		throw new CSVStreamException();
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.stream.CSVStreamException#CSVStreamException(java.lang.String)}.
	 */
	@Test
	public final void testCSVStreamExceptionString() {
		thrown.expectMessage("Test message");
		thrown.expect(CSVStreamException.class);
		throw new CSVStreamException("Test message");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.stream.CSVStreamException#CSVStreamException(java.lang.Throwable)}.
	 */
	@Test
	public final void testCSVStreamExceptionThrowable() {
		RuntimeException cause = new RuntimeException("Test exception cause");
		thrown.expectMessage("Test exception cause");
		thrown.expect(CSVStreamException.class);
		throw new CSVStreamException(cause);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.stream.CSVStreamException#CSVStreamException(java.lang.String, java.lang.Throwable)}.
	 */
	@Test
	public final void testCSVStreamExceptionStringThrowable() {
		RuntimeException cause = new RuntimeException("Test exception cause");
		thrown.expectMessage("Test message");
		thrown.expect(CSVStreamException.class);
		throw new CSVStreamException("Test message", cause);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.stream.CSVStreamException#CSVStreamException(java.lang.String, java.lang.Throwable, boolean, boolean)}.
	 */
	@Test
	public final void testCSVStreamExceptionStringThrowableBooleanBoolean() {
		RuntimeException cause = new RuntimeException("Test exception cause");
		thrown.expectMessage("Test message");
		thrown.expect(CSVStreamException.class);
		throw new CSVStreamException("Test message", cause, true, true);
	}

}
