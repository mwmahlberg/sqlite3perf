# sqlite3perf

This repository contains a small application which was created while researching a proper answer to the question [Faster sqlite 3 query in go? I need to process 1million+ rows as fast as possible][so:oq].

The assumption there was that Python is faster with accessing SQLite3 than Go is.

I wanted to check this and hence I wrote a generator for entries into an SQLite database as well as a Go implementation and a Python implementation of a simple access task:

1. Read all rows from table bench, which consists of an ID, a hex encoded 8 byte random value and a hex encoded SHA256 hash of said random values.
2. Create a SHA256 hex encoded checksum from the decoded random value of a row.
3. Compare the stored hash value against the generated one.
4. If they match, continue, otherwise throw an error.

[so:oq]: https://stackoverflow.com/questions/48000940/
