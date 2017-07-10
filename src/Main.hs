{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs,ExistentialQuantification #-}
{-# LANGUAGE DeriveFunctor, DeriveGeneric #-}
-- jup Persist needs quite some things
{-# LANGUAGE TemplateHaskell, QuasiQuotes, TypeFamilies, MultiParamTypeClasses, GeneralizedNewtypeDeriving #-}

module Main where

import           Control.Monad.Free (Free)
import qualified Control.Monad.Free as MF

import           Control.Monad.Logger (NoLoggingT)

import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Trans.Reader (ReaderT)
import           Control.Monad.Trans.Resource (ResourceT)

import           Control.Monad.Trans.State.Strict (StateT)
import qualified Control.Monad.Trans.State.Strict as State

import           Control.Monad.Trans.Writer.Strict (Writer)
import qualified Control.Monad.Trans.Writer.Strict as Writer

import           Data.Aeson (ToJSON(..),FromJSON(..))
import qualified Data.Aeson as Json
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as LBS
import           Data.List (foldl')
import           Data.Maybe (mapMaybe)
import           Data.Text (Text)

import           Database.Persist.TH (share, persistLowerCase
                                     , mkPersist, mkMigrate, sqlSettings)
import           Database.Persist.Sql (runMigration, SqlBackend)
import qualified Database.Persist.Sql as Sql
import           Database.Persist.Sqlite (runSqlite)

import           GHC.Generics


type EventStream ev = Free (EventStreamF ev)

-- | Functor-Interface for an event-stream
data EventStreamF ev a
  = AddEvent ev a
  | Project (Projection ev a)
  deriving Functor

----------------------------------------------------------------------
-- the DSL

addEvent :: ev -> EventStream ev ()
addEvent event = MF.liftF $ AddEvent event ()


project :: Projection ev res -> EventStream ev res
project = MF.liftF . Project


----------------------------------------------------------------------
-- projections

data Projection ev a =
  forall state . Projection { initState :: state
                            , fold :: state -> ev -> state
                            , final :: state -> a }


instance Functor (Projection ev) where
  fmap f (Projection i fd fi) = Projection i fd (f . fi)


----------------------------------------------------------------------
-- in Memory using State-Monad over a simple list

type MemoryStore ev = StateT [ev] (Writer [String])


runInMemory :: Show ev => [ev] -> EventStream ev res -> (res, [String])
runInMemory events = Writer.runWriter . flip State.evalStateT events . inMemory
  where
    inMemory = MF.iterM interpretMemory


interpretMemory :: Show ev =>
                   EventStreamF ev (MemoryStore ev res) -> MemoryStore ev res
interpretMemory (AddEvent ev mem) = do
  lift (Writer.tell ["adding Event " ++ show ev])
  State.withStateT (ev :) mem
interpretMemory (Project (Projection i fd fi)) = do
  lift (Writer.tell ["running projection"])
  events <- State.get
  fi (foldl' fd i events)


----------------------------------------------------------------------
-- interpret using a Persistent-Sqlite database

type SqliteStore ev = ReaderT SqlBackend (NoLoggingT (ResourceT IO))


share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
Event
    json ByteString
    deriving Show
|]


runInSqlite :: (ToJSON ev, FromJSON ev) => Text -> EventStream ev res -> IO res
runInSqlite connection query = do
  runSqlite connection $ do
    runMigration migrateAll
    inSqlite query
  where
    inSqlite = MF.iterM interpretPersitent


interpretPersitent :: (ToJSON ev, FromJSON ev) =>
                      EventStreamF ev (SqliteStore ev res) -> SqliteStore ev res
interpretPersitent (AddEvent ev query) = do
  Sql.insert $ Event (LBS.toStrict $ Json.encode ev)
  query
interpretPersitent (Project (Projection i fd fi)) = do
  events <- mapMaybe rowToEv <$> Sql.selectList [] []
  fi (foldl' fd i events)
  where
    rowToEv =  decodeJson . eventJson . Sql.entityVal
    decodeJson = Json.decode . LBS.fromStrict



----------------------------------------------------------------------
-- Example

data Events
  = NameSet String
  | AgeSet Int
  deriving (Generic, Show)


data Person
  = Person { name :: String, age :: Int }
  deriving Show


personP :: Projection Events Person
personP = Projection { initState = nobody, fold = foldEv, final = id }
  where
    nobody = Person "" 0
    foldEv person (NameSet n) = person { name = n }
    foldEv person (AgeSet a) = person { age = a }
    

example :: EventStream Events Person
example = do
  addEvent $ NameSet "Carsten"
  addEvent $ AgeSet 37
  project personP


----------------------------------------------------------------------
-- infrastructure

-- Events should be serializable to JSON

instance ToJSON Events where
    toEncoding = Json.genericToEncoding Json.defaultOptions

instance FromJSON Events

----------------------------------------------------------------------

dbConnection :: Text
dbConnection = ":memory:"

main :: IO ()
main = do
  let (result, log) = runInMemory [] example
  putStrLn "Log:"
  putStrLn $ unlines log
  putStrLn $ "\nResult:\n" ++ show result
  
  putStrLn "\n\n========\n\n"

  putStrLn "SQLite:"
  runInSqlite dbConnection example >>= print
