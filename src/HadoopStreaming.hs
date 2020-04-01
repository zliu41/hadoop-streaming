{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HadoopStreaming
  ( Mapper(..)
  , Reducer(..)
  , runMapper
  , runMapperWith
  , runReducer
  , runReducerWith
  , println
  , incCounter
  , incCounterBy
  , sourceHandle
  , sinkHandle
  ) where

import           Control.Monad.Extra (unlessM)
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
-- each input into a @(key, value)@ pair.
data Mapper e m = forall input k v. Mapper
  (Text -> Either e input)
  -- ^ Decoder for mapper input.
  (k -> v -> Text)
  -- ^ Encoder for mapper output.
  (ConduitT input (k, v) m ())
  -- ^ A stream transforming @input@ into @(k, v)@ pairs.

-- | A @Reducer@ consists of a decoder, an encoder, and a stream that transforms
-- each key and all values associated with the key into zero or more @res@.
data Reducer e m = forall k v res. Eq k => Reducer
  (Text -> Either e (k, v))
  -- ^ Decoder for reducer input
  (res -> Text)
  -- ^ Encoder for reducer output
  (k -> v -> ConduitT v res m ())
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

-- | Run a 'Mapper'. Takes input from 'stdin' and emits the result to 'stdout'.
--
-- > runMapper = runMapperWith (sourceHandle stdin) (sinkHandle stdout)
runMapper
  :: MonadIO m
  => (Text -> e -> m ())
  -- ^ A action to be executed for each input that cannot be decoded. The first parameter
  -- is the input and the second parameter is the decoding error. One may choose to, for instance,
  -- increment a counter and 'println' an error message.
  -> Mapper e m -> m ()
runMapper = runMapperWith stdin stdout

-- | Like 'runMapper', but allows specifying a source and a sink.
--
-- > runMapper = runMapperWith (sourceHandle stdin) (sinkHandle stdout)
runMapperWith
  :: MonadIO m
  => ConduitT () Text m ()
  -> ConduitT Text C.Void m ()
  -> (Text -> e -> m ())
  -> Mapper e m -> m ()
runMapperWith source sink f (Mapper dec enc trans) = runConduit $
    source .| CL.mapMaybeM g .| trans .| C.map (uncurry enc) .| sink
  where
    g input = either ((Nothing <$) . f input) (pure . Just) (dec input)

-- | Run a 'Reducer'. Takes input from 'stdin' and emits the result to 'stdout'.
--
-- > runReducer = runReducerWith (sourceHandle stdin) (sinkHandle stdout)
runReducer
  :: MonadIO m
  => (Text -> e -> m ())
  -- ^ A action to be executed for each input that cannot be decoded. The first parameter
  -- is the input and the second parameter is the decoding error. One may choose to, for instance,
  -- increment a counter and 'println' an error message.
  -> Reducer e m -> m ()
runReducer = runReducerWith stdin stdout

-- | Like 'runReducer', but allows specifying a source and a sink.
--
-- > runReducer = runReducerWith (sourceHandle stdin) (sinkHandle stdout)
runReducerWith
  :: MonadIO m
  => ConduitT () Text m ()
  -> ConduitT Text C.Void m ()
  -> (Text -> e -> m ())
  -> Reducer e m -> m ()
runReducerWith source sink f (Reducer dec enc trans) = runConduit $
    source .| CL.mapMaybeM g .| CL.groupOn1 fst .| reduceKey trans .| C.map enc .| sink
  where
    g input = either ((Nothing <$) . f input) (pure . Just) (dec input)

reduceKey :: forall m k v res. Monad m
          => (k -> v -> ConduitT v res m ())
          -> ConduitT ((k,v), [(k,v)]) res m ()
reduceKey f = go
  where
    go = C.await >>= maybe (pure ()) \((k, v), kvs) ->
      C.yieldMany (map snd kvs) .| f k v >> go

stdin :: MonadIO m => ConduitT i Text m ()
stdin = sourceHandle IO.stdin

stdout :: MonadIO m => ConduitT Text o m ()
stdout = sinkHandle IO.stdout

-- | Stream the contents of a 'IO.Handle' one line at a time as 'Text'.
sourceHandle :: MonadIO m => IO.Handle -> ConduitT i Text m ()
sourceHandle h = go .| C.filter (not . Text.all (== ' '))
  where
    go = unlessM (liftIO (IO.hIsEOF h)) (liftIO (Text.hGetLine h) >>= C.yield >> go)

-- | Stream data to a 'IO.Handle', separated by @\\n@.
sinkHandle :: MonadIO m => IO.Handle -> ConduitT Text o m ()
sinkHandle h = C.awaitForever (liftIO . Text.hPutStrLn h)

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
