{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

{- | Set of helpers helping with writing gRPC clients with not much exposure of
 the http2-client complexity.

 The GrpcClient handles automatic background connection-level window updates
 to prevent the connection from starving and pings to force a connection
 alive.

 There is no automatic reconnection, retry, or healthchecking. These features
 are not planned in this library and should be added at higher-levels.
-}
module Network.GRPC.Client.Helpers where

import Control.Concurrent.Async.Lifted (Async, async, cancel)
import Control.Concurrent.Lifted (threadDelay)
import Control.Exception (throwIO)
import Control.Monad (forever)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as ByteString
import Data.Default.Class (def)
#if MIN_VERSION_base(4,11,0)
#else
import Data.Monoid ((<>))
#endif

import Network.GRPC.Client
import Network.GRPC.HTTP2.Encoding
import Network.HPACK (HeaderList)
import Network.HTTP2.Client (ClientError, ClientIO, FallBackFrameHandler, GoAwayHandler, HostName, Http2Client (..), IncomingFlowControl (..), PortNumber, TooMuchConcurrency, frameHttp2RawConnection, ignoreFallbackHandler, newHttp2Client, newHttp2FrameConnection)
import Network.HTTP2.Client.Helpers (ping)
import Network.HTTP2.Client.RawConnection (newRawHttp2ConnectionSocket, newRawHttp2ConnectionUnix)
import qualified Network.Socket as Network
import qualified Network.TLS as TLS
import qualified Network.TLS.Extra.Cipher as TLS

{- | A simplified gRPC Client connected via an HTTP2Client to a given server.
 Each call from one client will share similar headers, timeout, compression.
-}
data GrpcClient = GrpcClient
    { -- | Underlying HTTP2 client.
      _grpcClientHttp2Client :: Http2Client
    , -- | Authority header of the server the client is connected to.
      _grpcClientAuthority :: Authority
    , -- | Extra HTTP2 headers to pass to every call (e.g., authentication tokens).
      _grpcClientHeaders :: [(ByteString, ByteString)]
    , -- | Timeout for RPCs.
      _grpcClientTimeout :: Timeout
    , -- | Compression shared for every call and expected for every answer.
      _grpcClientCompression :: Compression
    , -- | Running background tasks.
      _grpcClientBackground :: BackgroundTasks
    }

data BackgroundTasks = BackgroundTasks
    { -- | Periodically give the server credit to use the connection.
      backgroundWindowUpdate :: Async (Either ClientError ())
    , -- | Periodically ping the server.
      backgroundPing :: Async (Either ClientError ())
    }

{- | A generalized address that supports new TCP connections, new UNIX socket
 connections or using already connected sockets
-}
data Address
    = AddressTCP HostName PortNumber
    | AddressUnix FilePath
    | AddressSocket Network.Socket Authority

-- | Configuration to setup a GrpcClient.
data GrpcClientConfig = GrpcClientConfig
    { -- | Address of the server
      _grpcClientConfigAddress :: !Address
    , -- | Extra HTTP2 headers to pass to every call (e.g., authentication tokens).
      _grpcClientConfigHeaders :: ![(ByteString, ByteString)]
    , -- | Timeout for RPCs.
      _grpcClientConfigTimeout :: !Timeout
    , -- | Compression shared for every call and expected for every answer.
      _grpcClientConfigCompression :: !Compression
    , -- | TLS parameters for the session.
      _grpcClientConfigTLS :: !(Maybe TLS.ClientParams)
    , -- | HTTP2 handler for GoAways.
      _grpcClientConfigGoAwayHandler :: GoAwayHandler
    , -- | HTTP2 handler for unhandled frames.
      _grpcClientConfigFallbackHandler :: FallBackFrameHandler
    , -- | Delay in microsecond between to window updates.
      _grpcClientConfigWindowUpdateDelay :: Int
    , -- | Delay in microsecond between to pings.
      _grpcClientConfigPingDelay :: Int
    }

grpcClientConfigSimple :: HostName -> PortNumber -> UseTlsOrNot -> GrpcClientConfig
grpcClientConfigSimple host port tls =
    GrpcClientConfig (AddressTCP host port) [] (Timeout 3000) gzip (tlsSettings tls host port) (liftIO . throwIO) ignoreFallbackHandler 5000000 1000000

type UseTlsOrNot = Bool

tlsSettings :: UseTlsOrNot -> HostName -> PortNumber -> Maybe TLS.ClientParams
tlsSettings False _ _ = Nothing
tlsSettings True host port =
    Just $
        TLS.ClientParams
            { TLS.clientWantSessionResume = Nothing
            , TLS.clientUseMaxFragmentLength = Nothing
            , TLS.clientServerIdentification = (host, ByteString.pack $ show port)
            , TLS.clientUseServerNameIndication = True
            , TLS.clientShared = def
            , TLS.clientHooks =
                def
                    { TLS.onServerCertificate = \_ _ _ _ -> return []
                    }
            , TLS.clientSupported = def {TLS.supportedCiphers = TLS.ciphersuite_default}
            , TLS.clientDebug = def
#if MIN_VERSION_tls(1,5,0)
        , TLS.clientEarlyData            = Nothing
#endif
            }

setupGrpcClient :: GrpcClientConfig -> ClientIO GrpcClient
setupGrpcClient config = do
    let addr = _grpcClientConfigAddress config
    let tls = _grpcClientConfigTLS config
    let compression = _grpcClientConfigCompression config
    let onGoAway = _grpcClientConfigGoAwayHandler config
    let onFallback = _grpcClientConfigFallbackHandler config
    let timeout = _grpcClientConfigTimeout config
    let headers = _grpcClientConfigHeaders config
    let authority = case addr of
            AddressTCP host port -> ByteString.pack $ host <> ":" <> show port
            AddressUnix _ -> ByteString.pack "localhost"
            AddressSocket _ auth -> auth

    conn <- case addr of
        AddressTCP host port -> newHttp2FrameConnection host port tls
        AddressUnix path -> frameHttp2RawConnection =<< newRawHttp2ConnectionUnix path tls
        AddressSocket sock _ -> frameHttp2RawConnection =<< newRawHttp2ConnectionSocket sock tls
    cli <- newHttp2Client conn 8192 8192 [] onGoAway onFallback
    wuAsync <- async $
        forever $ do
            threadDelay $ _grpcClientConfigWindowUpdateDelay config
            _updateWindow $ _incomingFlowControl cli
    pingAsync <- async $
        forever $ do
            threadDelay $ _grpcClientConfigPingDelay config
            ping cli 3000000 "grpc.hs"
    let tasks = BackgroundTasks wuAsync pingAsync
    return $ GrpcClient cli authority headers timeout compression tasks

-- | Cancels background tasks and closes the underlying HTTP2 client.
close :: GrpcClient -> ClientIO ()
close grpc = do
    cancel $ backgroundPing $ _grpcClientBackground grpc
    cancel $ backgroundWindowUpdate $ _grpcClientBackground grpc
    _close $ _grpcClientHttp2Client grpc

-- | Run an unary query.
rawUnary ::
    (GRPCInput r i, GRPCOutput r o) =>
    -- | The RPC to call.
    r ->
    -- | An initialized client.
    GrpcClient ->
    -- | The input.
    i ->
    ClientIO (Either TooMuchConcurrency (RawReply o))
rawUnary rpc (GrpcClient client authority headers timeout compression _) input =
    let call = singleRequest rpc input
     in open client authority headers (Just timeout) (Encoding compression) (Decoding compression) call

-- | Calls for a server stream of requests.
rawStreamServer ::
    (GRPCInput r i, GRPCOutput r o) =>
    -- | The RPC to call.
    r ->
    -- | An initialized client.
    GrpcClient ->
    -- | An initial state.
    a ->
    -- | The input of the stream request.
    i ->
    -- | A state-passing handler called for each server-sent output.
    -- Headers are repeated for convenience but are the same for every iteration.
    (a -> HeaderList -> o -> ClientIO a) ->
    ClientIO (Either TooMuchConcurrency (a, HeaderList, HeaderList, IO (Either ClientError ())))
rawStreamServer rpc (GrpcClient client authority headers timeout compression _) v0 input handler =
    let call = streamReply rpc v0 input handler
     in open client authority headers (Just timeout) (Encoding compression) (Decoding compression) call

{- | Sends a streams of requests to the server.

 Messages are submitted to the HTTP2 underlying client and hence this
 function can block until the HTTP2 client has some network credit.
-}
rawStreamClient ::
    (GRPCInput r i, GRPCOutput r o) =>
    -- | The RPC to call.
    r ->
    -- | An initialized client.
    GrpcClient ->
    -- | An initial state.
    a ->
    -- | A state-passing step function to decide the next message.
    (a -> ClientIO (a, Either StreamDone (CompressMode, i))) ->
    ClientIO (Either TooMuchConcurrency (a, RawReply o))
rawStreamClient rpc (GrpcClient client authority headers timeout compression _) v0 getNext =
    let call = streamRequest rpc v0 getNext
     in open client authority headers (Just timeout) (Encoding compression) (Decoding compression) call

{- | Starts a bidirectional ping-pong like stream with the server.

 This handler is well-suited when the gRPC application has a deterministic
 protocols, that is, when after sending a message a client can know how many
 messages to wait for before sending the next message.
-}
rawSteppedBidirectional ::
    (GRPCInput r i, GRPCOutput r o) =>
    -- | The RPC to call.
    r ->
    -- | An initialized client.
    GrpcClient ->
    -- | An initial state.
    a ->
    -- | The sequential program to iterate between sending and receiving messages.
    RunBiDiStep i o a ->
    ClientIO (Either TooMuchConcurrency a)
rawSteppedBidirectional rpc (GrpcClient client authority headers timeout compression _) v0 handler =
    let call = steppedBiDiStream rpc v0 handler
     in open client authority headers (Just timeout) (Encoding compression) (Decoding compression) call

{- | Starts a stream with the server.

 This handler allows to concurrently write messages and wait for incoming
 messages.
-}
rawGeneralStream ::
    (GRPCInput r i, GRPCOutput r o) =>
    -- | The RPC to call.
    r ->
    -- | An initialized client.
    GrpcClient ->
    -- | An initial state for the incoming loop.
    a ->
    -- | A state-passing function for the incoming loop.
    (a -> IncomingEvent o a -> ClientIO a) ->
    -- | An initial state for the outgoing loop.
    b ->
    -- | A state-passing function for the ougoing loop.
    (b -> ClientIO (b, OutgoingEvent i b)) ->
    ClientIO (Either TooMuchConcurrency (a, b))
rawGeneralStream rpc (GrpcClient client authority headers timeout compression _) v0 handler w0 next =
    let call = generalHandler rpc v0 handler w0 next
     in open client authority headers (Just timeout) (Encoding compression) (Decoding compression) call
