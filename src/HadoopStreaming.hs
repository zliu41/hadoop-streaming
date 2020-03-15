{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module HadoopStreaming
  ( Mapper(..)
  , runMapper
  , println
  , incCounter
  , incCounter'
  ) where

import           Control.Exception.Extra (handle_)
import           Control.Monad.Extra
import           Control.Monad.IO.Class
import           Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.IO as Text
import           Pipes (Consumer', Pipe, Producer', runEffect, (>->))
import qualified Pipes
import qualified Pipes.Prelude as Pipes
import           System.Exit (die)
import qualified System.IO as IO

data Mapper m = forall input k v. Mapper
  (Text -> input)
  -- ^ Decoder for mapper input
  (k -> v -> Text)
  -- ^ Encoder for mapper output
  (Pipe input (k, v) m ())
  -- ^ A 'Pipe' transforming @input@ into @(k, v)@ pairs.

runMapper :: MonadIO m => Mapper m -> m ()
runMapper (Mapper dec enc trans) = runEffect $
  stdin >-> Pipes.map dec >-> trans >-> Pipes.map (uncurry enc) >-> stdout

stdin :: MonadIO m => Producer' Text m ()
stdin = unlessM (liftIO IO.isEOF) (liftIO Text.getLine >>= Pipes.yield >> stdin)

stdout :: MonadIO m => Consumer' Text m r
stdout = Pipes.for Pipes.cat (liftIO . Text.putStrLn)

-- | Like 'Text.putStrLn', but writes to 'stderr'.
println :: MonadIO m => Text -> m ()
println = liftIO . Text.hPutStrLn IO.stderr

-- | Increment a counter by 1.
incCounter
  :: MonadIO m
  => Text -- ^ Group name. Must not contain comma.
  -> Text -- ^ Counter name. Must not contain comma.
  -> m ()
incCounter group name = incCounter' group name 1

-- | Increment a counter by @n@.
incCounter'
  :: MonadIO m
  => Text -- ^ Group name. Must not contain comma.
  -> Text -- ^ Counter name. Must not contain comma.
  -> Int
  -> m ()
incCounter' group name n = println $
  Text.intercalate "," [group, name, Text.pack (show n)]
