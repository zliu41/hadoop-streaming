A simple Hadoop streaming library based on [conduit](https://hackage.haskell.org/package/conduit),
useful for writing mapper and reducer logic in Haskell and running it on AWS Elastic MapReduce,
Azure HDInsight, GCP Dataproc, and so forth.

# A Few Things to Note

## ByteString vs Text

The `HadoopStreaming` module provides the general `Mapper` and `Reducer` data types, where the input and output
types of the mapper and reducer are abstract. They are usually instantiated with either `ByteString` or `Text`.
`ByteString` is more suitable if the input/output needs to be decoded/encoded, for instance using the
`base64-bytestring` library. On the other hand, `Text` could make more sense if you decoding/encoding is not needed,
or if the data is not UTF-8 encoded (see below regarding encodings). In general I'd imagine `ByteString` being
used much more often than `Text`.

The `HadoopStreaming.ByteString` and `HadoopStreaming.Text` modules provide some utilities for working with
`ByteString` and `Text`, respectively.

## Encoding

Hackage: https://hackage.haskell.org/package/hadoop-streaming
