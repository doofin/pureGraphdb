# Minimalistic graph database implemented in haskell

inspired by neo4j.

# feature
store only node id,no properties/attributes,etc. is stored.

a monadic interface

o(k) (locally constant) to find neighbour node

add,modify,delete node and edge
  
# dependency

aeson (run : cabal install aeson) , for serialization

# usage

Load into ghci,then

create node : 
```haskell
runGdb $ createModifyNode (node..)
```

create edge from nodeid 0 to nodeid 12 : 
```haskell
runGdb $ createModifyEdgeT $ Edge Nothing (Just 0) (Just 12) "" 0
```

# TODO
Integration with a external key-value db to store extra information for node and edge
