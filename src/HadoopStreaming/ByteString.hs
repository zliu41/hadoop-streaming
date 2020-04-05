{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TupleSections #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  HadoopStreaming.ByteString
-- Maintainer  :  Ziyang Liu <free@cofree.io>
--
-- This module has some utilities for working with 'ByteString' in Hadoop streaming.
module HadoopStreaming.ByteString
  ( sourceHandle
  , sinkHandle
  , stdinLn
  , stdoutLn
  , defaultKeyValueEncoder
  , defaultKeyValueDecoder
  ) where

import           Conduit (MonadThrow(..), lift)
import           Control.Exception (IOException, try)
import           Control.Monad.Extra (unlessM, (>=>))
import           Control.Monad.IO.Class (MonadIO(..))
import           Data.Conduit (ConduitT, (.|))
import qualified Data.Conduit as C
import qualified Data.Conduit.Combinators as C
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BC
import qualified System.IO as IO


-- | Stream the contents of a 'IO.Handle' one line at a time as 'ByteString'.
--
-- __NB__: This only works if the input from the 'IO.Handle' is UTF-8 encoded.
sourceHandle
  :: MonadIO m => IO.Handle -> ConduitT i ByteString m ()
sourceHandle h = go .| C.filter (not . BC.all (== ' '))
  where
    go = unlessM (liftIO (IO.hIsEOF h)) (liftIO (BC.hGetLine h) >>= C.yield >> go)

-- | Stream data to a 'IO.Handle', separated by @\\n@.
--
-- __NB__: This only works if the data is UTF-8 encoded.
sinkHandle :: MonadIO m => IO.Handle -> ConduitT ByteString o m ()
sinkHandle h = C.awaitForever (liftIO . BC.hPutStrLn h)

-- | Stream the contents from 'System.IO.stdin' one line at a time as 'ByteString'.
--
-- __NB__: This only works if the input from the 'IO.Handle' is UTF-8 encoded.
--
-- > stdinLn = sourceHandle System.IO.stdin
stdinLn :: (MonadIO m, MonadThrow m) => ConduitT i ByteString m ()
stdinLn = sourceHandle IO.stdin

-- | Stream data to 'System.IO.stdout', separated by @\\n@.
--
-- __NB__: This only works if the data is UTF-8 encoded.
--
-- > stdoutLn = sinkHandle System.IO.stdout
stdoutLn :: MonadIO m => ConduitT ByteString o m ()
stdoutLn = sinkHandle IO.stdout

-- | Encode a key-value pair by separating them with a (UTF-8 encoded) tab (i.e., a @0x09@ byte),
-- which is the default way the mapper output should be formatted.
defaultKeyValueEncoder
  :: (k -> ByteString)
  -- ^ Key encoder
  -> (v -> ByteString)
  -- ^ Value encoder
  -> k -> v -> ByteString
defaultKeyValueEncoder encKey encValue k v = encKey k <> "\t" <> encValue v

-- | Decode a line by treating the prefix up to the first tab as key, and the suffix after the first
-- tab as value. If the line does not contain a tab, or if the first tab is the last character,
-- the whole line is considered as key, and the value decoder is not used.
--
-- __NB__: This only works if the data is UTF-8 encoded.
defaultKeyValueDecoder
  :: (ByteString -> Either e k)
  -- ^ Key decoder
  -> (ByteString -> Either e v)
  -- ^ Value decoder
  -> ByteString -> Either e (k, Maybe v)
defaultKeyValueDecoder decKey decValue i
    | BC.length i2 <= 1 = (,Nothing) <$> decKey i1
    | otherwise = do
        k <- decKey i1
        v <- decValue (BC.tail i2)
        pure (k, Just v)
  where
    (i1, i2) = BC.break (== '\t') i
