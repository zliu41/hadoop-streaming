A simple Hadoop streaming library based on [conduit](https://hackage.haskell.org/package/conduit),
useful for writing mapper and reducer logic in Haskell and running it on AWS Elastic MapReduce,
Azure HDInsight, GCP Dataproc, and so forth.

Hackage: https://hackage.haskell.org/package/hadoop-streaming

# Word Count Example

See the [Haddock in `HadoopStreaming.Text`](https://hackage.haskell.org/package/hadoop-streaming/docs/HadoopStreaming-Text.html)
for a simple word-count example.

# A Few Things to Note

## ByteString vs Text

The `HadoopStreaming` module provides the general `Mapper` and `Reducer` data types, whose input and output
types are abstract. They are usually instantiated with either `ByteString` or `Text`.
`ByteString` is more suitable if the input/output needs to be decoded/encoded, for instance using the
`base64-bytestring` library. On the other hand, `Text` could make more sense if decoding/encoding is not needed,
or if the data is not UTF-8 encoded (see below regarding encodings). In general I'd imagine `ByteString` being
used much more often than `Text`.

The `HadoopStreaming.ByteString` and `HadoopStreaming.Text` modules provide some utilities for working with
`ByteString` and `Text`, respectively.

## Encoding

It is highly recommended that your input data be UTF-8 encoded, as this is the default encoding Hadoop uses. If you
must use other encodings such as UTF-16, keep in mind the following gotchas:

- It is not enough that your code can work with the encoding you choose to use:

  - By default, if any of your input files does not end with a UTF-8 representation of newline, i.e., a `0x0A` byte, Hadoop
    streaming will add a `0x0A` byte.

  - Likewise, if any line in your mapper output does not contain a UTF-8 representation of tab (`0x09`), Hadoop streaming will
    add it at the end of the line.

  This will almost certainly break your job. It may be possible to configure Hadoop streaming and tell it to use other encodings,
  so that the above behavior is consistent with the encoding you choose to use, but I don't know whether that is the case. I tried
  `-D mapreduce.map.java.opts="-Dfile.encoding=UTF-16BE"` but that doesn't seem to work.

- If you use `ByteString` as the input type and use `Data.ByteString.hGetLine` to read lines from the input, be
  aware that `Data.ByteString.hGetLine` uses `0x0A` bytes as line breaks, so it doesn't work properly for
  non-UTF-8 encoded input. For example, in UTF-16BE and UTF-16LE, the newline character is encoded as `0x00 0x0A` and `0x0A 0x00`,
  respectively.
