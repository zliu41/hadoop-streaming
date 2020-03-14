{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}

module HadoopStreaming
  ( Mapper(..)
  , runMapper
  , runMapper'
  , runMapperIO
  , runMapperIO'
  ) where

import           Control.Exception.Extra (handle_)
import           Control.Monad.Extra
import           Control.Monad.IO.Class
import           Data.Text (Text)
import qualified Data.Text.IO as Text
import           Pipes (Consumer', Pipe, Producer', runEffect, (>->))
import qualified Pipes
import qualified Pipes.Prelude as Pipes
import           System.Exit (die)
import           System.IO (isEOF)

data Mapper m = forall input k v. Mapper
  (Text -> input)
  -- ^ Decoder for mapper input
  ((k, v) -> Text)
  -- ^ Encoder for mapper output
  (forall r. Pipe input (k, v) m r)
  -- ^ A 'Pipe' transforming @input@ into @(k, v)@ pairs.

runMapper :: MonadIO m => Mapper m -> m ()
runMapper = runMapper' (pure ()) (pure ())

runMapper' :: MonadIO m => m () -> m () -> Mapper m -> m ()
runMapper' before after (Mapper dec enc trans) = do
  before
  runEffect $ stdin >-> Pipes.map dec >-> trans >-> Pipes.map enc >-> stdout
  after

runMapperIO :: String -> Mapper IO -> IO ()
runMapperIO task = runMapperIO' task (pure ()) (pure ())

runMapperIO' :: String -> IO () -> IO () -> Mapper IO -> IO ()
runMapperIO' task before after = handle_ handler . runMapper' before after
  where
    handler e = die $ "Map task " ++ task ++ " failed: " ++ show e

stdin :: MonadIO m => Producer' Text m ()
stdin = unlessM (liftIO isEOF) (liftIO Text.getLine >>= Pipes.yield >> stdin)

stdout :: MonadIO m => Consumer' Text m r
stdout = Pipes.for Pipes.cat (liftIO . Text.putStrLn)
