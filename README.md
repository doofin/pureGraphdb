# Minimalistic graph database implemented completely in haskell,inspired by neo4j.
Not a wrapper for neo4j! 

# feature

o(1) to find neighbour node

add,modify,delete node and edge
  
# dependency

aeson (run cabal install aeson) , for serialization

# usage

Load into ghci,then

create node : runGdb $ createModifyNode (node..)

create edge from nodeid 0 to nodeid 12 : runGdb $ createModifyEdgeT $ Edge Nothing (Just 0) (Just 12) "" 0

# TODO
Integration with a external key-value db to store any information in node and edge
