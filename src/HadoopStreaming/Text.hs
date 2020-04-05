{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  HadoopStreaming.Text
-- Maintainer  :  Ziyang Liu <free@cofree.io>
--
-- This module has some utilities for working with 'Text' in Hadoop streaming.
--
-- Word count example:
--
-- @
--  {-\# LANGUAGE OverloadedStrings, TupleSections \#-}
--
--  import Data.Conduit (ConduitT)
--  import qualified Data.Conduit as C
--  import qualified Data.Conduit.Combinators as C
--  import Data.Void (Void)
--  import HadoopStreaming
--  import qualified HadoopStreaming.Text as HT
--  import Data.Text (Text)
--  import qualified Data.Text as Text
--
--  mapper :: Mapper Text Text Void IO
--  mapper = Mapper dec enc trans
--    where
--      dec :: Text -> Either Void [Text]
--      dec = Right . Text.words
--
--      enc :: Text -> Int -> Text
--      enc = HT.defaultKeyValueEncoder id (Text.pack . show)
--
--      trans :: ConduitT [Text] (Text, Int) IO ()
--      trans = C.concatMap (map (,1))
--
--  reducer :: Reducer Text Text Void IO
--  reducer = Reducer dec enc trans
--    where
--      dec :: Text -> Either Void (Text, Int)
--      dec i = Right (i1, read . tail . Text.unpack $ i2)
--        where (i1, i2) = Text.break (== '\\t') i
--
--      enc :: (Text, Int) -> Text
--      enc (t, c) = t <> "," <> Text.pack (show c)
--
--      trans :: Text -> Int -> ConduitT Int (Text, Int) IO ()
--      trans k v0 = C.foldl (+) v0 >>= C.yield . (k,)
-- @
module HadoopStreaming.Text
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
import           Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.IO as Text
import qualified System.IO as IO


-- | Stream the contents of a 'IO.Handle' one line at a time as 'Text'.
sourceHandle
  :: MonadIO m
  => (IOException -> m ())
  -- ^ An action to be executed if there is an error reading the input. This is usually
  -- caused by the input having an incorrect encoding or containing corrupt data.
  -- The recommended action is to log an error message and fail the job.
  --
  -- __NB__: The stream will terminate if an error occurrs, regardless of whether this
  -- action re-throws the error or not.
  -> IO.Handle -> ConduitT i Text m ()
sourceHandle f h = go .| C.filter (not . Text.all (== ' '))
  where
    go = unlessM (liftIO (IO.hIsEOF h)) $
      liftIO (try @IOException (Text.hGetLine h)) >>= either (lift . f) (C.yield >=> const go)

-- | Stream data to a 'IO.Handle', separated by @\\n@.
sinkHandle :: MonadIO m => IO.Handle -> ConduitT Text o m ()
sinkHandle h = C.awaitForever (liftIO . Text.hPutStrLn h)

-- | Stream the contents from 'System.IO.stdin' one line at a time as 'Text'.
--
-- > stdinLn = sourceHandle throwM System.IO.stdin
stdinLn :: (MonadIO m, MonadThrow m) => ConduitT i Text m ()
stdinLn = sourceHandle throwM IO.stdin

-- | Stream data to 'System.IO.stdout', separated by @\\n@.
--
-- > stdoutLn = sinkHandle System.IO.stdout
stdoutLn :: MonadIO m => ConduitT Text o m ()
stdoutLn = sinkHandle IO.stdout

-- | Encode a key-value pair by separating them with a tab, which is the default way
-- the mapper output should be formatted.
defaultKeyValueEncoder
  :: (k -> Text)
  -- ^ Key encoder
  -> (v -> Text)
  -- ^ Value encoder
  -> k -> v -> Text
defaultKeyValueEncoder encKey encValue k v = encKey k <> "\t" <> encValue v

-- | Decode a line by treating the prefix up to the first tab as key, and the suffix after the first
-- tab as value. If the line does not contain a tab, or if the first tab is the last character,
-- the whole line is considered as key, and the value decoder is not used.
defaultKeyValueDecoder
  :: (Text -> Either e k)
  -- ^ Key decoder
  -> (Text -> Either e v)
  -- ^ Value decoder
  -> Text -> Either e (k, Maybe v)
defaultKeyValueDecoder decKey decValue i
    | Text.length i2 <= 1 = (,Nothing) <$> decKey i1
    | otherwise = do
        k <- decKey i1
        v <- decValue (Text.tail i2)
        pure (k, Just v)
  where
    (i1, i2) = Text.break (== '\t') i
