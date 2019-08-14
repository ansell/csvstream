module com.github.ansell.csv.stream {
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.core.filter;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.dataformat.csv;

    exports com.github.ansell.csv.stream;
    exports com.github.ansell.csv.stream.util;
}
