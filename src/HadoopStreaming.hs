{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module HadoopStreaming
  ( Mapper(..)
  , Reducer(..)
  , runMapper
  , runReducer
  , println
  , incCounter
  , incCounterBy
  ) where

import           Control.Monad.IO.Class (MonadIO(..))
import           Data.Conduit (ConduitT, runConduit, (.|))
import qualified Data.Conduit as C
import qualified Data.Conduit.Combinators as C
import qualified Data.Conduit.List as CL
import           Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.IO as Text
import qualified System.IO as IO


-- | A @Mapper@ consists of a decoder, an encoder, and a stream that transforms
-- input into @(key, value)@ pairs.
data Mapper i o e m = forall j k v. Mapper
  (i -> Either e j)
  -- ^ Decoder for mapper input
  (k -> v -> o)
  -- ^ Encoder for mapper output
  (ConduitT j (k, v) m ())
  -- ^ A stream transforming @input@ into @(k, v)@ pairs.

-- | A @Reducer@ consists of a decoder, an encoder, and a stream that transforms
-- each key and all values associated with the key into some result values.
data Reducer i o e m = forall k v r. Eq k => Reducer
  (i -> Either e (k, v))
  -- ^ Decoder for reducer input
  (r -> o)
  -- ^ Encoder for reducer output
  (k -> v -> ConduitT v r m ())
  -- ^ A stream processing a key and all values associated with the key. The parameter
  -- @v@ is the first value associated with the key (since a key always has one or more
  -- values), and the remaining values are processed by the conduit.
  --
  -- Examples:
  --
  -- @
  -- import qualified Data.Conduit as C
  -- import qualified Data.Conduit.Combinators as C
  --
  -- -- Sum up all values associated with the key and emit a (key, sum) pair.
  -- sumValues :: (Monad m, Num v) => k -> v -> ConduitT v (k, v) m ()
  -- sumValues k v0 = C.foldl (+) v0 >>= C.yield . (k,)
  --
  -- -- Increment a counter for each (key, value) pair, and emit the (key, value) pair.
  -- incCounterAndEmit :: MonadIO m => k -> v -> ConduitT v (k, v) m ()
  -- incCounterAndEmit k v0 = C.leftover v0 <> C.mapM \\v ->
  --   incCounter "reducer" "key-value pairs" >> pure (k, v)
  -- @


-- | Run a 'Mapper'.
runMapper
  :: MonadIO m
  => ConduitT () i m ()
  -- ^ Mapper source. The source should generally stream from @stdin@, and produce
  -- a value for each line of the input, as that is how Hadoop streaming is supposed
  -- to work, but this is not enforced. For example, if you really want to, you can produce a single
  -- value for the entire input split (which can be done using @Data.Conduit.Combinators.stdin@
  -- as the source). Note that regardless of what the source does, the Hadoop counter
  -- @"Map-Reduce Framework: Map input records"@ is always the number of lines of the input, because
  -- this counter is managed by the Hadoop framework.
  --
  -- An example is 'HadoopStreaming.Text.stdin', which streams from @stdin@ as 'Text', one line
  -- at a time.
  -> ConduitT o C.Void m ()
  -- ^ Mapper sink. It should generally stream to @stdout@. An example is
  -- 'HadoopStreaming.Text.stdout'.
  -> (i -> e -> m ())
  -- ^ An action to be executed for each input that cannot be decoded. The first parameter
  -- is the input and the second parameter is the decoding error. One may choose to, for instance,
  -- increment a counter and 'println' an error message.
  -> Mapper i o e m -> m ()
runMapper source sink f (Mapper dec enc trans) = runConduit $
    source .| CL.mapMaybeM g .| trans .| C.map (uncurry enc) .| sink
  where
    g input = either ((Nothing <$) . f input) (pure . Just) (dec input)

-- | Run a 'Reducer'.
runReducer
  :: MonadIO m
  => ConduitT () i m ()
  -- ^ Reducer source
  -> ConduitT o C.Void m ()
  -- ^ Reducer sink
  -> (i -> e -> m ())
  -- ^ An action to be executed for each input that cannot be decoded.
  -> Reducer i o e m -> m ()
runReducer source sink f (Reducer dec enc trans) = runConduit $
    source .| CL.mapMaybeM g .| CL.groupOn1 fst .| reduceKey trans .| C.map enc .| sink
  where
    g input = either ((Nothing <$) . f input) (pure . Just) (dec input)

reduceKey
  :: Monad m
  => (k -> v -> ConduitT v r m ())
  -> ConduitT ((k,v), [(k,v)]) r m ()
reduceKey f = go
  where
    go = C.await >>= maybe (pure ()) \((k, v), kvs) ->
      C.yieldMany (map snd kvs) .| f k v >> go

-- | Like 'Text.putStrLn', but writes to 'stderr'.
println :: MonadIO m => Text -> m ()
println = liftIO . Text.hPutStrLn IO.stderr

-- | Increment a counter by 1.
incCounter
  :: MonadIO m
  => Text -- ^ Group name. Must not contain comma.
  -> Text -- ^ Counter name. Must not contain comma.
  -> m ()
incCounter = incCounterBy 1

-- | Increment a counter by @n@.
incCounterBy
  :: MonadIO m
  => Int
  -> Text -- ^ Group name. Must not contain comma.
  -> Text -- ^ Counter name. Must not contain comma.
  -> m ()
incCounterBy n group name = println $
  "reporter:counter:" <> Text.intercalate "," [group, name, Text.pack (show n)]
