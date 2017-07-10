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
import           Data.Int (Int64)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (mapMaybe, fromMaybe)
import           Data.Text (Text)

import           Database.Persist.TH (share, persistLowerCase
                                     , mkPersist, mkMigrate, sqlSettings)
import           Database.Persist.Sql (runMigration, SqlBackend, (==.))
import qualified Database.Persist.Sql as Sql
import           Database.Persist.Sqlite (runSqlite)

import           GHC.Generics


type AggregateKey = Int64

type EventStream ev = Free (EventStreamF ev)

-- | Functor-Interface for an event-stream
data EventStreamF ev a
  = NewAggregate (AggregateKey -> a)
  | AddEvent AggregateKey ev a
  | Project AggregateKey (Projection ev a)
  deriving Functor

----------------------------------------------------------------------
-- the DSL

newAggregate :: EventStream ev AggregateKey
newAggregate = MF.liftF $ NewAggregate id


addEvent :: AggregateKey -> ev -> EventStream ev ()
addEvent key event = MF.liftF $ AddEvent key event ()


project :: AggregateKey -> Projection ev res -> EventStream ev res
project key = MF.liftF . Project key


----------------------------------------------------------------------
-- projections

data Projection ev a =
  forall state . Projection { initState :: AggregateKey -> state
                            , fold :: state -> ev -> state
                            , final :: state -> a }


instance Functor (Projection ev) where
  fmap f (Projection i fd fi) = Projection i fd (f . fi)


----------------------------------------------------------------------
-- in Memory using State-Monad over a simple list

type MemoryStore ev = StateT (Map AggregateKey [ev]) (Writer [String])


runInMemory :: Show ev => (Map AggregateKey [ev]) -> EventStream ev res -> (res, [String])
runInMemory events = Writer.runWriter . flip State.evalStateT events . inMemory
  where
    inMemory = MF.iterM interpretMemory


interpretMemory :: Show ev =>
                   EventStreamF ev (MemoryStore ev res) -> MemoryStore ev res
interpretMemory (NewAggregate mem) = do
  key <- fromIntegral . (+1) <$> State.gets Map.size
  lift (Writer.tell ["starting new Aggregate with key " ++ show key])
  State.withStateT (Map.insert key []) (mem key)
interpretMemory (AddEvent key ev mem) = do
  lift (Writer.tell ["adding to Aggregate " ++ show key ++ " event " ++ show ev])
  State.withStateT (Map.adjust (ev:) key) mem
interpretMemory (Project key (Projection i fd fi)) = do
  lift (Writer.tell ["running projection on Aggregate " ++ show key])
  events <- fromMaybe [] <$> State.gets (Map.lookup key)
  fi (foldr (flip fd) (i key) events)


----------------------------------------------------------------------
-- interpret using a Persistent-Sqlite database

type SqliteStore ev = ReaderT SqlBackend (NoLoggingT (ResourceT IO))


share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
Event
    aggId Int64
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
interpretPersitent (NewAggregate cont) = do
  id <- next . map (eventAggId . Sql.entityVal) <$>  Sql.selectList [] [Sql.Desc EventAggId, Sql.LimitTo 1]
  Sql.insert (Event id "") -- insert an empty entry to reserve the key
  cont id
  where
    next [] = 1
    next (nr:_) = nr + 1
interpretPersitent (AddEvent id ev query) = do
  Sql.insert $ Event id (LBS.toStrict $ Json.encode ev)
  query
interpretPersitent (Project key (Projection i fd fi)) = do
  events <- mapMaybe rowToEv <$> Sql.selectList [EventAggId ==. key] []
  fi (foldr (flip fd) (i key) events)
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
  = Person
    { key :: AggregateKey
    , name :: String
    , age :: Int
    }
  deriving Show


personP :: Projection Events Person
personP = Projection { initState = nobody, fold = foldEv, final = id }
  where
    nobody key = Person key "" 0
    foldEv person (NameSet n) = person { name = n }
    foldEv person (AgeSet a) = person { age = a }
    

example :: EventStream Events Person
example = do
  key <- newAggregate
  addEvent key $ NameSet "Carsten"
  addEvent key $ AgeSet 37
  project key personP


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
  let (result, log) = runInMemory Map.empty example
  putStrLn "Log:"
  putStrLn $ unlines log
  putStrLn $ "\nResult:\n" ++ show result
  
  putStrLn "\n\n========\n\n"

  putStrLn "SQLite:"
  runInSqlite dbConnection example >>= print
