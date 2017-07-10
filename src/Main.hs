{-# LANGUAGE GADTs #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE DeriveFunctor #-}

module Main where

import           Control.Monad.Free (Free)
import qualified Control.Monad.Free as MF

import           Control.Monad.Trans.Class (lift)

import           Control.Monad.Trans.State.Strict (StateT)
import qualified Control.Monad.Trans.State.Strict as State

import           Control.Monad.Trans.Writer.Strict (Writer)
import qualified Control.Monad.Trans.Writer.Strict as Writer

import Data.List (foldl')


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


execInMemory :: Show ev => [ev] -> EventStream ev res -> ([ev], [String])
execInMemory events = Writer.runWriter . flip State.execStateT events . inMemory


inMemory :: Show ev => EventStream ev res -> MemoryStore ev res
inMemory = MF.iterM interpretMemory


interpretMemory :: Show ev => EventStreamF ev (MemoryStore ev res) -> MemoryStore ev res
interpretMemory (AddEvent ev mem) = do
  lift (Writer.tell ["adding Event " ++ show ev])
  State.withStateT (ev :) mem
interpretMemory (Project (Projection i fd fi)) = do
  lift (Writer.tell ["running projection"])
  events <- State.get
  fi (foldl' fd i events)


----------------------------------------------------------------------
-- Example

data Events
  = NameSet String
  | AgeSet Int
  deriving Show


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

main :: IO ()
main = do
  let (result, log) = runInMemory [] example
  putStrLn "Log:"
  putStrLn $ unlines log
  putStrLn $ "\nResult:\n" ++ show result
