{-# LANGUAGE TupleSections, ViewPatterns #-}

module HadoopStreamingSpec where

import qualified Data.Conduit as C
import qualified Data.Conduit.Combinators as C
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import           System.IO.Extra (IOMode(..), withFile, withTempFile)

import           HadoopStreaming
import           HadoopStreaming.ByteString

import           Test.Hspec


ignoreDecodeError :: a -> b -> IO ()
ignoreDecodeError _ _ = pure ()

spec :: Spec
spec =
  describe "Testing HadoopStreaming" $ do
    it "test case 1 - mapper" $ do
      let fin = "test/resource/1.in"
          fout = "test/resource/1.mapper-out"
      actual <- withFile fin ReadMode $ \hin ->
        withTempFile $ \temp -> do
          withFile temp WriteMode $ \hout -> do
            let mapper = Mapper dec enc trans
                  where
                    dec :: ByteString -> Either String (Int, Int)
                    dec = Right . read . B.unpack

                    enc :: Int -> Int -> ByteString
                    enc x y = B.pack $ show x ++ ", " ++ show y

                    trans = C.map $ \(x, y) -> (x + 1000, y + 100)
            runMapper (sourceHandle hin) (sinkHandle hout) ignoreDecodeError mapper
          readFile temp
      expected <- readFile fout

      actual `shouldBe` expected

    it "test case 1 - reducer" $ do
      let fin = "test/resource/1.in"
          fout = "test/resource/1.reducer-out"
      actual <- withFile fin ReadMode $ \hin ->
        withTempFile $ \temp -> do
          withFile temp WriteMode $ \hout -> do
            let reducer = Reducer dec enc trans
                  where
                    dec :: ByteString -> Either String (Int, Int)
                    dec = Right . read . B.unpack

                    enc :: (Int, Int) -> ByteString
                    enc (x, y) = B.pack $ show x ++ ", " ++ show y

                    trans = \k v -> C.foldl (+) v >>= C.yield . (k,)
            runReducer (sourceHandle hin) (sinkHandle hout) ignoreDecodeError reducer
          readFile temp
      expected <- readFile fout

      actual `shouldBe` expected

    it "test case 1 - mapper - odd keys" $ do
      let fin = "test/resource/1.in"
          fout = "test/resource/1.mapper-out-oddkeys"
      actual <- withFile fin ReadMode $ \hin ->
        withTempFile $ \temp -> do
          withFile temp WriteMode $ \hout -> do
            let mapper = Mapper dec enc trans
                  where
                    dec :: ByteString -> Either () (Int, Int)
                    dec (read . B.unpack -> (x, y))
                      | odd x = Right (x, y)
                      | otherwise = Left ()

                    enc :: Int -> Int -> ByteString
                    enc x y = B.pack $ show x ++ ", " ++ show y

                    trans = C.map $ \(x, y) -> (x + 1000, y + 100)
            runMapper (sourceHandle hin) (sinkHandle hout) ignoreDecodeError mapper
          readFile temp
      expected <- readFile fout

      actual `shouldBe` expected
