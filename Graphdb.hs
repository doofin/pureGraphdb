{-# LANGUAGE ConstraintKinds,OverloadedStrings,StandaloneDeriving,FlexibleInstances,TypeSynonymInstances,DeriveDataTypeable,DataKinds,TypeFamilies,MultiParamTypeClasses,DeriveGeneric #-}
{-# OPTIONS_GHC -fno-warn-unused-do-bind#-}


module Graphdb where

import Control.Monad.IO.Class
import Control.Monad.Trans.State
import Control.Monad
import Control.Exception

import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Word
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as B
import Data.Binary.Get
import Data.Binary.Put
import Data.ByteString.Builder
import Data.Maybe
import Data.Aeson
import Data.List as L
import GHC.Generics
import Debug.Trace
import System.IO

import qualified Database.Redis as Kv

type Uid=BlkUnit
type PostiveInt=Integer
type BlkUnit=Word64
type Btype=BlkUnit
type UidNode=Uid
type UidEdge=Uid

data Blk=Blk {uid::Uid,--uid for edge must be more than 0!
              btype::Btype,--For node and edge:type
              pnode::UidNode,--For node:pointer node,For edge:also points to  node
              pedge::UidEdge,--For node:first edge,if no edge then 0 ,for edge:used as pointer to next edge
              pext::Uid--external res 
             } deriving (Show,Generic,Eq)


type BlkNode=Blk
type BlkEdge=Blk
type PosBlk=PostiveInt

data Node=Node {nuid::Maybe UidNode,
                nName::String,
                nExtInfo::BlkUnit} deriving (Show,Generic,Eq)
type NodeE=(Node,ExtInfo)
instance ToJSON Node where
    toJSON (Node (Just uid)  nm eifo)=object ["id".= uid,"nm".=nm]
instance FromJSON Node 


--  toJSON n=object ["uid".=(fromJust $ nuid n)]

data Edge=Edge {euid::Maybe UidEdge,
                suid::Maybe UidNode,
                tuid::Maybe UidNode,
                eName::String,
                eExtInfo::BlkUnit} deriving (Show,Generic)
instance ToJSON Edge where
  toJSON (Edge e s t nm ext)=object ["id".= e,"s".=s,"t".=t,"nm".=nm]

type Graph=([Node],[Edge])


data ExtInfo=Ebs B.ByteString|Ename T.Text
         deriving Show
type Extkey=B.ByteString





data GdbState=GdbState {nodesFile::Handle, --edgesFile::Handle,
                        freeuidFile::Handle,
                        incruidFile::Handle,
                        redisconn::Kv.Connection}

type GdbMonad m=MonadIO m
type GdbStateT m a=StateT GdbState m a
type GdbStateIO a =GdbStateT IO a

data Uidaction=UidFree Uid|UidUse deriving Show
btype_node=0;btype_edge=1;btype_edgetrace=2
pedge_nil=0;pnode_nil=0


blktst=Blk 0 0 0 0 0
nodetst=Node (Just 1) "nodeDummy" 0
nodenewtst=Node (Nothing) "nodeDummy" 0
nodentst=(nodetst,T.pack "nodeN test")


word64tobs::Word64->B.ByteString
word64tobs=BL.toStrict .  toLazyByteString . word64BE

getWord64::Handle->IO Word64
getWord64 hd=do
  hSeek hd AbsoluteSeek 0
  bs<-BL.hGet  hd 8
  return $ runGet getWord64be bs
setWord64::Handle->Word64->IO ()
setWord64 hd w=do
  --hSetFileSize hd 0
  hSeek hd AbsoluteSeek 0
  BL.hPut hd $ runPut $ putWord64be w

putWord64::Handle->Word64->IO ()
putWord64 hd w=do
    BL.hPut hd $ runPut $ putWord64be w
printFileInfo=runGdb $ get>>=liftIO.getWord64.incruidFile
uidManager::Uidaction->GdbStateIO BlkUnit
uidManager UidUse=do 
  ifo<-get
  let fhd=freeuidFile ifo
      incr=incruidFile ifo
  fhdsz<-liftIO $ hFileSize fhd
  uid<-liftIO $ do
    if fhdsz==0
      then do
      incrfsz<-hFileSize incr
      if incrfsz==0
        then setWord64 incr 0 >>return (0::Word64)
        else getWord64 incr>>= (\x->do --bug here,solved by hseek to 0!
                                   let y=x+1
                                   setWord64 incr y
                                   return (y ::Word64))
      else liftIO $ getAuid fhd
  liftIO $ mapM_ hFlush [fhd,incr]
  return uid
  where
    getAuid::Handle->IO Uid
    getAuid hd=do
      hSeek hd SeekFromEnd (-8)
      res<-getWord64 hd
      hFileSize hd>>=(\sz->hSetFileSize hd $ sz-8) --hClose hd
      return res --deleteNode uid=uidManager (UidFree uid) 

uidManager (UidFree uidf) = do
  ifo<-get
  let fhd=freeuidFile ifo
  liftIO $ putWord64 fhd uidf
  return uidf

delBlk::Uid->GdbStateIO ()
delBlk u =do
  uidManager $ UidFree u
  runkvdb $  Kv.del [word64tobs u]
  return ()
readBlk::PosBlk->GdbStateT IO Blk
readBlk pos=do
  h<- get>>=return .nodesFile 
  liftIO $ do
    hSetBinaryMode h True
    hSeek h AbsoluteSeek $ pos*8*5
    bs<-BL.hGetContents  h
    runGet
          (do
            a<-getWord64be
            b<-getWord64be
            c<-getWord64be
            d<-getWord64be
            e<-getWord64be
            return $ return $ Blk a b c d e)
          bs
    --hFlush h
--    print res
  --  return res


writeBlk::Blk->GdbStateT IO ()
writeBlk (Blk a b c d e) =do
--  uid<-uidManager UidUse
  h<-getNodeHandles
  liftIO $ do
    hSetBinaryMode h True
    hSeek h AbsoluteSeek $ fromIntegral $  a*5*8
    BL.hPut h $ runPut $ do --putWord64be a
      mapM_ putWord64be [a,b,c,d,e] --hClose h

getExtbs::Uid->GdbStateIO (Maybe ExtInfo)
getExtbs uid=do
  val<-runkvdb $ Kv.get $ BL.toStrict $  toLazyByteString $ word64BE uid
  return $ (fromEither val)>>=(\x->Just $ Ebs x)

getExtName::Uid->GdbStateIO ExtInfo
getExtName uid=do
  res<-getExtbs uid
  let Ebs x=fromMaybe (Ebs "no ext name!!") res
  return $ Ename $ T.decodeUtf8 x
getExtN::Uid->GdbStateIO T.Text
getExtN uid=do
  res<-getExtbs uid
  let Ebs x=fromMaybe (Ebs "no ext name!") res
  return $  T.decodeUtf8 x


setExtinfo::Uid->ExtInfo->GdbStateIO ()
setExtinfo uid (Ebs bs)=do
  runkvdb $ Kv.set (BL.toStrict $  toLazyByteString $ word64BE $ uid) bs
  return ()
setExtinfo uid (Ename bs)=do
  runkvdb $ Kv.set (word64tobs uid) $ T.encodeUtf8 bs
  return ()

newNodeE::NodeE->GdbStateIO UidNode
newNodeE (nd,ifo)=do
  nid<-createModifyNode nd
  setExtinfo nid ifo
  return nid
newNode::Node->GdbStateIO UidNode
newNode nd=do
  nid<-createModifyNode nd
  setExtinfo nid $ Ename $ T.pack $ nName nd
  return nid

runkvdb::Kv.Redis a->GdbStateIO a
runkvdb dbact=do
  st<-get
  liftIO $ Kv.runRedis (redisconn st) dbact

getAllBlk::IO ()
getAllBlk=runGdb $ do
  ifo<-get
  (readAll $ nodesFile ifo)>>=(\blks->liftIO . (\(ns,es)->mapM_ print ns>>putStrLn "\n">>mapM_ print es)   $ partition (\x-> (btype x)==btype_node) blks)

readAll::Handle->GdbStateIO [Blk]
readAll hd=do
  len<-liftIO $ hFileSize hd
  sequence $ fmap (readBlk) $ gl $ quot len $ 8*5
  where
    gl 1=[0]
    gl n=(n-1):(gl $ n-1)

putWord64ls::Handle->[Word64]->IO ()
putWord64ls hd ws=do
  mapM_ (\w->putWord64 hd w) ws
  hSetFileSize hd $ fromIntegral ((length ws)*8)
  --hClose hd
getWord64ls::Handle->IO [Word64]
getWord64ls hd =do
  len<-hFileSize hd
  sequence $ fmap (const $ getWord64 hd) $ gl $ quot len 8
  where
    gl 1=[0]
    gl n=0:(gl $ n-1)


fromEither::Show a=> Either a b->b
fromEither ei=case ei of
               Right u->u
               Left e->trace (show e) $ error "fromEither error"
redset=do
  runKvdb $ do
    Kv.set "h" "he"
redget=do
  runKvdb $ do
    Kv.get "h"
    
runKvdb::Kv.Redis a->IO a
runKvdb act=do
  conn <- Kv.connect Kv.defaultConnectInfo {Kv.connectPort=Kv.PortNumber 6379}
  Kv.runRedis conn act

runGdb::GdbStateIO a->IO a
runGdb act=liftIO $ bracket
  (liftIO $ do
      hs<-sequence $ fmap (\f->openFile f ReadWriteMode) ["gnodes","gfreeuid","gincruid"]
      conn <- liftIO $ Kv.connect Kv.defaultConnectInfo
      return (hs,conn)
  )
  (\r-> do
      mapM_ hClose . fst $ r
  )
  (\(hs ,conn)->let nf:ff:incrf:[]=hs in
                  evalStateT act $  GdbState nf ff incrf conn)

getNodeHandles::GdbMonad m=>GdbStateT m Handle
getNodeHandles=get>>=return.nodesFile

createModifyNode::Node->GdbStateT IO Uid
createModifyNode (Node muid nm extifo)=do
  let preblk uid' =Blk uid' 0 0 0 extifo
  maybe
    (uidManager UidUse >>=(\uid->writeBlk (preblk uid) >>return uid)>>=return)
    (\uid->writeBlk (preblk uid)>>return uid)
    muid


createModifyEdge'::Edge->Btype->GdbStateT IO UidEdge
createModifyEdge' (Edge euid (Just nd1) (Just nd2) nm extinfo) bt =do
  nd1blk<-readBlk $ toInteger nd1
  let trac::BlkNode->GdbStateIO Blk --follow node to the last edge
      trac blk= if ((pedge blk)==pedge_nil) then return blk else readBlk (toInteger $ pedge blk)>>=trac
      makeedge uid=do
        nd1lastedge<-trac nd1blk
        mapM writeBlk [Blk uid bt nd2 0 extinfo,nd1lastedge{pedge=uid}]--add edge blk ,, modify pointer in node for edge
        return uid
  maybe (uidManager UidUse>>=makeedge) makeedge euid

createModifyEdgeT'::Edge->GdbStateT IO (UidEdge,UidEdge)
createModifyEdgeT' (Edge euid (Just nd1) (Just nd2) nm extinfo)  =do
  nd1blk<-readBlk $ toInteger nd1
  nd2blk<-readBlk $ toInteger nd2
  let trac::BlkNode->GdbStateIO Blk --follow node to the last edge
      trac blk= if ((pedge blk)==pedge_nil) then return blk else readBlk (toInteger $ pedge blk)>>=trac
      makeedge uid initblk bt=do
        nd1lastedge<-trac initblk
        mapM writeBlk [Blk uid bt nd2 0 extinfo,nd1lastedge{pedge=uid}]--add edge blk ,, modify pointer in node for edge
        return uid
  maybe (do
            u1<-uidManager UidUse
            makeedge u1 nd1blk btype_edge
            u2<-uidManager UidUse
            makeedge u2 nd2blk btype_edgetrace
            return (u1,u2)
        )
    (\u->do
        u1<-makeedge u nd1blk btype_edge
        return (u1,-1)
    )
    euid

--create "traced" edge in order to be fetched by getting neighbours,accessable from nd1 with btype_edge,nd2 with btype_edgetrace --the edge created is only accessable from one direction,not to be used for other operation
createModifyEdgeT::Edge->GdbStateIO UidEdge
createModifyEdgeT edg@(Edge _ s t _ _) =do
  u<-createModifyEdge' edg btype_edge
  v<-createModifyEdge' edg{euid=Nothing,suid=t,tuid=s} btype_edgetrace --error here no enough bytes for get read,maybe due to read immediate after write
  liftIO $ print (u,v)
  return u
testcreateedge=runGdb $ createModifyEdgeT $ Edge Nothing (Just 0) (Just 12) "" 0
createBranch::Edge->Node->GdbStateT IO (UidNode,UidEdge)
createBranch edge@(Edge _ se te _ _) n2=do --se and te can not be nothing at the same time!
  n2id<-createModifyNode n2
  if (se,te)==(Nothing,Nothing) then error "se and te can not be nothing at the same time!" else return ()
  let (s,t)=if se==Nothing then (Just n2id,te) else (se,Just n2id)
  liftIO $ do putStrLn $ "branch from s to t:" ++ (show (s,t))
  eid<-createModifyEdgeT edge{euid=Nothing,suid=s,tuid=t}
  return (n2id,eid)
getNode::Uid->GdbStateT IO Node
getNode uid= do
  r<-readBlk (fromIntegral uid)
  return $  Node  (Just uid)  "null" $  pext r

getNodeE::Uid->GdbStateT IO NodeE
getNodeE uid=do
  nd<-getNode uid
  mifo<-getExtbs uid
  let res= (nd,fromMaybe (Ebs "null") mifo)
  return res

getEdgeBlk::UidEdge->GdbStateIO BlkEdge
getEdgeBlk uid=readBlk $ fromIntegral uid

traceblk::[BlkNode]->GdbStateIO [BlkEdge]--get all edges of a node
traceblk alln@(n:_) =do
        if  (pedge n ==pedge_nil) &&
            (or [btype n ==btype_edge,btype n==btype_edgetrace])
          then return alln--singleton node,edge without further successor
          else do
          new<-getEdgeBlk $ pedge n
          traceblk $ new:alln

propagate::([BlkNode],[BlkEdge])->GdbStateIO ([BlkNode],[BlkEdge]) -- category node->enclosing nodes and edge
propagate old@(ns ,es)=do
  ls<-sequence $ fmap getNbBlks ns
  let (nns,nes)=(\(x,y)-> (nub $ join x,nub $ join y)) $ unzip ls
  if (nns,nes)==old then return old else propagate (nns,nes)

propagate2::[(BlkNode,[BlkEdge])]->GdbStateIO [(BlkNode,[BlkEdge])]
propagate2 =undefined
  
getAcat::UidNode->GdbStateIO Graph --virtual node for grouping ,return enclosing whole graph,incomplete!
getAcat nid =do
  ls<-readBlk (fromIntegral nid)>>=getNbBlks2>>=fmap (nubBy (\(x,_) (y,_)->x==y)) . propagate2. return
  blkinit<-readBlk  (fromIntegral nid)
  propagate ([blkinit],[])>>= \r->return $ blk2graph $ toMatched r


--get neighbour nodes   and edges
getNbG::UidNode->GdbStateT IO Graph
getNbG un= readBlk  (fromIntegral un)>>= \blkinit ->do
  (resnodes,resedges)<-getNbBlks blkinit
  return $ unzip $ fmap (\(e,n)->(Node (Just $ uid n) "nodename" 0,
                                    let (s,t)=if (btype e)==btype_edgetrace  then (pnode e,uid blkinit) else (uid blkinit,pnode e) in
                                    Edge (Just $ uid e) (Just $ s) (Just $ t) "edgenm" 0)) $ zipWith (,) resedges resnodes


getNbBlks::BlkNode->GdbStateIO ([BlkNode],[BlkEdge]) 
getNbBlks blkinit=do
  if or [pedge blkinit==pedge_nil,btype blkinit==btype_edge,btype blkinit==btype_edgetrace]
    then return $ ([],[])
    else do
    resedges<- fmap init (traceblk [blkinit])
    resnodes<-sequence $ fmap (\bk->readBlk $ fromIntegral $ pnode bk) resedges
    return (resnodes,resedges)
getNbBlks2::BlkNode->GdbStateIO (BlkNode,[BlkEdge]) --center->[(center,edges to other)]
getNbBlks2 blkinit=do
  if or [pedge blkinit==pedge_nil,btype blkinit==btype_edge,btype blkinit==btype_edgetrace]
    then return (blkinit,[])
    else do
    resedges<- fmap init (traceblk [blkinit])
    return (blkinit,resedges)
  

toMatched::([BlkNode],[BlkEdge])->[(BlkNode,BlkEdge)] --invariant! the order of the list!
toMatched =uncurry  zip

toGraph::[(BlkNode,[BlkEdge])]->GdbStateIO Graph
toGraph xs=do
  xss<-sequence $ fmap (\(n,es)-> (sequence $ fmap (readBlk.fromIntegral.pnode) es)>>=(\e->return (n,e))) xs
  let   nds=fmap (\(n,_)->Node (Just $ uid n) "nodename" $ pext n) xss
  return undefined

blk2graph::[(BlkNode,BlkEdge)]->Graph
blk2graph xs= let ys=fmap (\(Blk nuid _ npnode npedge next,Blk euid btype epnode epedge eext)-> 
                             (Node (Just nuid) "nodename" next,
                              let (s,t) = if btype==btype_edgetrace then (epnode,nuid) else (nuid,epnode)
                              in Edge (Just euid) (Just s) (Just t) "edgename" eext)
                    ) xs
              in unzip  ys
test::IO ()
test=do
  getAllBlk>>=print
  runGdb $ do getNbG 0 >>=liftIO.mapM_ print



{-
runGdb_old::GdbMonad m=>GdbStateT m a->m a
runGdb_old gdb=do
  --liftIO $ system "redis/redis-server"
  nf:ff:incrf:[]<-liftIO $ sequence $ fmap (\f->openFile f ReadWriteMode) ["gnodes","gfreeuid","gincruid"]
  conn <- liftIO $ Kv.connect Kv.defaultConnectInfo
  res<-evalStateT gdb $  GdbState nf ff incrf conn
  liftIO $ do
--    mapM_ hFlush [nf,ff,incrf]
--    mapM_ hClose [nf,ff,incrf]
    return ()
  return res

readBlk::FilePath->PosBlk->IO Blk
readBlk fp pos=withFile fp ReadMode $ \h->do
  hSetBinaryMode h True
  hSeek h AbsoluteSeek $ pos*8*3
  bs<-B.hGetContents h
  let res=runGet 
        (do
            a<-getWord64be
            b<-getWord64be
            c<-getWord64be
            return $ Blk a b c)
        bs
  print res
  return res

writeBlk::FilePath->Blk->PosBlk->IO ()
writeBlk fp blk pos=withFile fp ReadWriteMode $ \h->do
  hSetBinaryMode h True
  hSeek h AbsoluteSeek $ pos*3*8
  B.hPut h $ runPut $ do
    putWord64be $ uid blk
    putWord64be $ pedge blk
    putWord64be $ pext blk
  hClose h

getNeighbours::UidNode->GdbStateT IO [Node]
getNeighbours un=do
  let neig::[BlkNode]->GdbStateIO [BlkNode]
      neig alln@(n:_) 
        =do
        liftIO $ putStrLn $ "getnei:"++ (show n)
        if any id [(pedge n ==pedge_nil) && (btype n ==btype_edge)]--singleton node,edge without further successor,
          then return alln
          else do
          new<-getEdgeBlk $ pedge n
          neig $ new:alln

  blkinit<-readBlk $ fromIntegral un
  if pedge blkinit==pedge_nil then return [] else do
    res''<- neig [blkinit]
    res'<-sequence $ fmap (\bk->readBlk $ fromIntegral $ pnode bk) $ init res''
    liftIO $ print res'
    return $ fmap (\x->Node (Just $ uid x) (pext x)) res'
createEdge::UidNode->Edge->UidNode->GdbStateT IO UidEdge
createEdge nd1 (Edge euid _ _ extinfo) nd2=do
  let nd1blkm=readBlk $ toInteger nd1
      newEdgeblk uid nd=Blk uid btype_edge nd 0 extinfo
      trac::GdbStateT IO BlkNode->GdbStateT IO Blk
      trac mblk=do
              (edgenil,_,egid)<-mblk>>=(\blk->return  ((pedge blk)==pedge_nil,uid blk,pedge blk))
              if edgenil then mblk else trac $ readBlk (toInteger egid)
      makeedge uid=do
        nd1blk<-trac nd1blkm
        writeBlk $ newEdgeblk uid nd2
        writeBlk $ nd1blk{pedge=uid}
        return uid
  maybe
    (uidManager UidUse>>=makeedge)
    makeedge
    euid


-}
