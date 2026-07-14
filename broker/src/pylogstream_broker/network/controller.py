from pylogstream_broker.config import BrokerConfig, ControllerAddress
from pylogstream_protocol import response, commands as command, encoder, parser, framer
from pylogstream_protocol.common import PacketHeader
import asyncio
from typing import TYPE_CHECKING, Tuple
from uuid import uuid4

if TYPE_CHECKING:
    from pylogstream_broker.replication.manager import ReplicaManager

class ControllerConnection:
    def __init__(self, config: 'BrokerConfig', replica_manager: 'ReplicaManager'):
        """
        Call ControllerConnection.connect() to create an instance of this class. This constructor is private and should not be called directly.
        """
        self.config = config
        self.replica_manager = replica_manager
        self._heartbeat_loop_task: asyncio.Task | None = None
        self._reader_loop_task: asyncio.Task | None = None
        self._connection_retry_task: asyncio.Task | None = None
        self._connected: bool = False
        self._reconnecting: bool = False
        self._connection_id: int = 0

        self._conn_status_event:asyncio.Event = asyncio.Event()

        self._conn_disconnect_event: asyncio.Event = asyncio.Event()

        self._pending_reads: dict[str, asyncio.Future] = {}

        self._lock = asyncio.Lock()
        self._reconnect_lock = asyncio.Lock()
    
    @classmethod
    async def connect(cls, config: 'BrokerConfig', replica_manager: 'ReplicaManager') -> 'ControllerConnection':
        instance = cls(config, replica_manager)
        await instance._start()
        return instance
    
    async def _start(self):
        if self._connected:
            return  
        reader, writer  = await self._connect_loop()
        self._reader = reader
        self._writer = writer


        self._connected = True
        self._conn_status_event.set()  # Signal that the connection is established
        self._conn_disconnect_event.clear()  # Clear the disconnect event since we are now connected

        self._connection_retry_task = asyncio.create_task(self._connection_retry_loop())
        self._heartbeat_loop_task = asyncio.create_task(self._heartbeat_loop())
        self._reader_loop_task = asyncio.create_task(self._reader_loop())

        # Connection established
        self.replica_manager.on_isr_change = self.update_isr
    
    async def __aenter__(self):
        await self._start()
        return self
    
    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def close(self):
        self._connected = False
        self.replica_manager.on_isr_change = None  # Remove the callback to avoid further ISR updates
        if self._reader_loop_task:
            self._reader_loop_task.cancel()
        if self._connection_retry_task:
            self._connection_retry_task.cancel()
        if self._heartbeat_loop_task:
            self._heartbeat_loop_task.cancel()
        
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()

        for fut in self._pending_reads.values():
            if not fut.done():
                fut.cancel()
        self._pending_reads.clear()

        self._conn_status_event.clear() 
        self._conn_disconnect_event.set()  

    @property
    def connected(self) -> bool:
        return self._connected
    
    @property
    def connection_id(self) -> int:
        return self._connection_id

    @property
    def reconnecting(self) -> bool:
        return self._reconnecting

    async def register_topic(self, topic: str):
        cmd = command.RegisterTopicCommand(topic=topic)
        resp = await self._send_command_loop(cmd, await_response=True)
        return resp
        

    async def update_isr(self, topic: str, isr_list: list[str]):
        cmd = command.ISRChangeCommand(topic=topic, isr_list=isr_list)
        await self._send_command_loop(cmd, await_response=False)
    
    async def request_metadata(self, topic: str):
        cmd = command.MetadataRequestCommand(topic=topic)
        resp = await self._send_command_loop(cmd, await_response=True)
        return resp
    
    async def _handle_disconnect(self):
        """
        Handle the disconnection await till the connection is re-established.
        """
        if not self._connected:
            return  # Already disconnected, no need to handle again
        curr_id = self._connection_id
        self._conn_disconnect_event.set()  # Signal that the connection is lost
        await self._wait_for_connection(stale_id=curr_id)
    
    async def _connection_retry_loop(self):
        """
        Acts as a service continuously trying to maintain a connection to the controller.
        """
        while self.connected:
            await self._conn_disconnect_event.wait()  # Wait until the connection is lost before retrying
            if not self.connected:
                break
            try:
                async with self._reconnect_lock:
                    self._conn_disconnect_event.clear()
                    self._conn_status_event.clear()  # Clear the connection status event before attempting to connect
                    self._reconnecting = True

                    for fut in self._pending_reads.values():
                        if not fut.done():
                            fut.set_exception(ConnectionError("Connection lost during pending read"))
                    self._pending_reads.clear()

                    reader, writer = await self._connect_loop()
                    self._reader = reader
                    self._writer = writer
                    self._connected = True
                    self._reconnecting = False
                    self._conn_status_event.set()  # Signal that the connection is established
                    self._connection_id += 1
            except Exception as e:
                print(f"Failed to connect to controller: {e}")
                await asyncio.sleep(1)  # Wait before retrying

    
    async def _connect_loop(self) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        controller_list = self.config.controller_list

        leader_hint = None
        while True:
            # Try the first hint
            if leader_hint:
                result = await self._try_controller(leader_hint)
                if isinstance(result, response.NotLeaderControllerResponse):
                    if result.leader_id and result.leader_host!='0.0.0.0' and result.leader_port!=0:
                        # Update the hint to the new leader
                        leader_hint = ControllerAddress(
                            controller_id=result.leader_id,
                            host=result.leader_host,
                            port=result.leader_port
                        )
                        continue
                if result is not None and not isinstance(result, response.NotLeaderControllerResponse):
                    # Successfully connected to the leader controller
                    return result
                
                # Hint didn't work fallback to full recovery
                leader_hint = None
            for controller in controller_list:
                result = await self._try_controller(controller)
                if isinstance(result, response.NotLeaderControllerResponse):
                    if result.leader_id and result.leader_host!='0.0.0.0' and result.leader_port!=0:
                        # Update the hint to the new leader
                        leader_hint = ControllerAddress(
                            controller_id=result.leader_id,
                            host=result.leader_host,
                            port=result.leader_port
                        )
                        break  # Break to try the new leader hint
                if result is not None and not isinstance(result, response.NotLeaderControllerResponse):
                    # Successfully connected to the leader controller
                    return result
            await asyncio.sleep(1)  # Wait before retrying
        
    async def _try_controller(
            self, 
            controller, 
            timeout=2
            ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]|response.NotLeaderControllerResponse|None:
        try:
            reader, writer = await asyncio.wait_for(self._connect(controller), timeout=timeout)
            resp  = await asyncio.wait_for(self._read_response(reader), timeout=timeout)
            if isinstance(resp, response.NotLeaderControllerResponse):
                return resp
            if isinstance(resp, response.TopicMetaDataListResponse):
                await self.replica_manager.apply_metadata_list(resp)
            return reader, writer
        except (asyncio.TimeoutError, ConnectionError, OSError):
            return None
    
    async def _wait_for_connection(self, stale_id: int = -1):
        """
        Wait for connection to be established. If a stale_id is provided, it will only return if the connection_id is greater than stale_id.
        
        :param self: Description
        :param stale_id: Optional stale connection ID, only return if the connection_id is greater than stale_id
        :type stale_id: int
        """
        if not self.connected:
            raise RuntimeError("Controller is not running.")
        # Avoid waiting if already connected
        if self._connected and not self._reconnecting and self._connection_id > stale_id:
            return
        while True:
            await self._conn_status_event.wait()
            if self._connected and not self._reconnecting and self._connection_id > stale_id:
                return

    async def _read_response(self, reader: asyncio.StreamReader) -> response.Response|None:
        len_data = await reader.readexactly(framer.PREFIX_SIZE)
        if not len_data:
            return None
        length = framer.decode_length(len_data)
        data = await reader.readexactly(length)
        if not data:
            return None
        try:
            resp = parser.parse_response(data)

            # TODO: This is a temporary solution, implement a more robust way to handle responses that require additional payload reading.
            if isinstance(resp, response.TopicMetaDataListHeaderResponse):
                # If the response is a header, we need to read the payload
                payload_length = resp.payload_length
                meta_list = []
                if payload_length > 0:
                    payload_data = await reader.readexactly(payload_length)
                    if not payload_data:
                        return None
                    # Attach the payload to the response object
                    meta_list = parser.parse_metadata_list_payload(payload_data)
                new_resp = response.TopicMetaDataListResponse(meta_list=meta_list)
                # Attach the header to the new response
                new_resp.header = resp.header
                resp = new_resp
        except ValueError as e:
            print(f"Failed to parse response: {e}")
            # Restart the connection if parsing fails, as it may indicate a protocol mismatch or corruption
            return None
        return resp
    
    async def _send_command_loop(self, cmd: command.Command, await_response: bool = True) -> response.Response|None:
        """
        Send the command and wait for the response. If the connection is lost, it will attempt to reconnect and resend the command.
        """
        # It will indefinitely and if instance is not running it will raise RuntimeError
        while True:
            try:
                fut = await self._send_command(cmd, await_response)
                if fut is None:
                    return None

                return await fut

            except (ConnectionError, BrokenPipeError, ConnectionResetError, OSError):
                await self._handle_disconnect()
    
    async def _send_command(self, cmd: command.Command, await_response: bool = True) -> asyncio.Future|None:

        if cmd.header is None and await_response:
            # Generate a unique correlation ID for this command
            correlation_id = str(uuid4())  
            # Insert the correlation ID into the command's header
            cmd.header = PacketHeader(correlation_id=correlation_id)

        await self._wait_for_connection()  
        fut = None
        if await_response and cmd.header is not None:
            fut = asyncio.get_event_loop().create_future()
            self._pending_reads[cmd.header.correlation_id] = fut
        async with self._lock:
            # TODO: Make utility for encoding and sending which can handles file and memoryview payloads
            frame = encoder.encode_command(cmd)
            self._writer.write(frame.header)
            if frame.payload:
                self._writer.write(frame.payload)
        await self._writer.drain()
        if not fut:
            return None
        return fut
    
    async def _connect(self, controller) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.open_connection(controller.host, controller.port)
        reg_cmd = command.BrokerRegisterCommand(
            broker_id=self.replica_manager.broker_id,
            host=self.config.host,
            port=self.config.port
        )
        frame = encoder.encode_command(reg_cmd)
        writer.write(frame.header)
        if frame.payload:
            writer.write(frame.payload)
        await writer.drain()
        return reader, writer

    async def _heartbeat_loop(self):
        while self._connected:
            try:
                ping_cmd = command.ControllerPingCommand(broker_id=self.replica_manager.broker_id)
                await self._send_command_loop(ping_cmd, await_response=False)
                await asyncio.sleep(5)  # Heartbeat interval
            except asyncio.CancelledError:
                break  # Exit the loop if the task is cancelled
            except Exception as e:
                print(f"Unexpected error in heartbeat loop: {e}")
                await asyncio.sleep(1)  # Prevent tight loop on unexpected errors
    
    async def _reader_loop(self):
        while self._connected:
            try:
                resp = await self._read_response(self._reader)
                if resp is None:
                    raise ConnectionError("Disconnected from controller")

                if (correlation_id := resp.header.correlation_id if resp.header else None) and correlation_id in self._pending_reads:
                    future = self._pending_reads.pop(correlation_id)
                    if not future.done():
                        future.set_result(resp)
                else:
                    await self._handle_unsolicited_response(resp)
            except (
                asyncio.IncompleteReadError,
                ConnectionError,
                BrokenPipeError,
                ConnectionResetError,
                OSError
            ) as e:
                # Log the error and continue
                print("Connection to controller is lost or not usable. Attempting to reconnect...", e)
                await self._handle_disconnect()
            except asyncio.CancelledError:
                break  # Exit the loop if the task is cancelled
            except Exception as e:
                # Log unexpected exceptions and continue
                print(f"Unexpected error in reader loop: {e}")
                await asyncio.sleep(1)  # Prevent tight loop on unexpected errors
    
    async def _handle_unsolicited_response(self, resp: response.Response):
        match resp:
            case response.NotLeaderControllerResponse():
                print(f"Received NotLeaderControllerResponse: {resp}")
                await self._handle_disconnect()
            case response.TopicMetaDataResponse():
                await self.replica_manager.apply_metadata(resp)
            case response.TopicMetaDataListResponse():
                await self.replica_manager.apply_metadata_list(resp)
            case response.ControllerPingResponse():
                print("Received ControllerPingResponse")
        