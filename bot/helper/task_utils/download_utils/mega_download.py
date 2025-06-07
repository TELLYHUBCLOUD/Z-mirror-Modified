#!/usr/bin/env python3
import atexit
from secrets import token_urlsafe
from aiofiles.os import makedirs
from threading import Event
from typing import Optional

from mega import MegaApi, MegaError, MegaListener, MegaRequest, MegaTransfer

from bot import (LOGGER, config_dict, download_dict, download_dict_lock,
                 non_queued_dl, queue_dict_lock)
from bot.helper.ext_utils.bot_utils import (async_to_sync, get_mega_link_type,
                                            sync_to_async)
from bot.helper.ext_utils.task_manager import (is_queued, limit_checker,
                                               stop_duplicate_check)
from bot.helper.mirror_utils.status_utils.mega_download_status import MegaDownloadStatus
from bot.helper.mirror_utils.status_utils.queue_status import QueueStatus
from bot.helper.telegram_helper.message_utils import (auto_delete_message,
                                                      delete_links,
                                                      sendMessage,
                                                      sendStatusMessage)

class MegaAppListener(MegaListener):
    _NO_EVENT_ON = (MegaRequest.TYPE_LOGIN, MegaRequest.TYPE_FETCH_NODES)
    NO_ERROR = "no error"

    def __init__(self, continue_event: Event, listener):
        self.continue_event = continue_event
        self.node = None
        self.public_node = None
        self.listener = listener
        self.is_cancelled = False
        self.error = None
        self.__bytes_transferred = 0
        self.__speed = 0
        self.__name = ''
        super().__init__()

    @property
    def speed(self):
        return self.__speed

    @property
    def downloaded_bytes(self):
        return self.__bytes_transferred

    def onRequestStart(self, api, request):
        LOGGER.debug(f'Mega request started: {request.getType()}')

    def onRequestFinish(self, api, request, error):
        if str(error).lower() != self.NO_ERROR:
            self.error = error.copy()
            LOGGER.error(f'Mega request finished with error: {self.error}')
            self.continue_event.set()
            return

        request_type = request.getType()
        if request_type == MegaRequest.TYPE_LOGIN:
            api.fetchNodes()
        elif request_type == MegaRequest.TYPE_GET_PUBLIC_NODE:
            self.public_node = request.getPublicMegaNode()
            self.__name = self.public_node.getName()
        elif request_type == MegaRequest.TYPE_FETCH_NODES:
            LOGGER.info("Fetching Root Node.")
            self.node = api.getRootNode()
            self.__name = self.node.getName()
            LOGGER.info(f"Node Name: {self.node.getName()}")

        if request_type not in self._NO_EVENT_ON or (self.node and "cloud drive" not in self.__name.lower()):
            self.continue_event.set()

    def onRequestTemporaryError(self, api, request, error: MegaError):
        LOGGER.error(f'Mega temporary request error: {error}')
        if not self.is_cancelled:
            self.is_cancelled = True
            async_to_sync(self.listener.onDownloadError, f"RequestTempError: {error.toString()}")
        self.error = error.toString()
        self.continue_event.set()

    def onTransferStart(self, api, transfer):
        LOGGER.info(f'Transfer started: {transfer.getFileName()}')

    def onTransferUpdate(self, api: MegaApi, transfer: MegaTransfer):
        if self.is_cancelled:
            api.cancelTransfer(transfer, None)
            self.continue_event.set()
            return
        self.__speed = transfer.getSpeed()
        self.__bytes_transferred = transfer.getTransferredBytes()

    def onTransferFinish(self, api: MegaApi, transfer: MegaTransfer, error):
        try:
            if self.is_cancelled:
                self.continue_event.set()
                return

            if error and error.getErrorCode() != MegaError.API_OK:
                self.error = f"Transfer failed with error: {error.toString()}"
                LOGGER.error(self.error)
                async_to_sync(self.listener.onDownloadError, self.error)
                return

            if transfer.isFinished() and (transfer.isFolderTransfer() or transfer.getFileName() == self.__name):
                LOGGER.info(f'Transfer completed: {transfer.getFileName()}')
                async_to_sync(self.listener.onDownloadComplete)
        except Exception as e:
            LOGGER.error(f'Error in onTransferFinish: {e}')
            self.error = str(e)
            async_to_sync(self.listener.onDownloadError, f"Transfer finish error: {e}")
        finally:
            self.continue_event.set()

    def onTransferTemporaryError(self, api, transfer, error):
        filen = transfer.getFileName()
        state = transfer.getState()
        errStr = error.toString()
        LOGGER.error(f'Mega temporary transfer error in {filen}: {errStr}')

        if state in [1, 4]:  # Queued or retrying
            return

        self.error = f"TransferTempError: {errStr} ({filen})"
        if not self.is_cancelled:
            self.is_cancelled = True
            async_to_sync(self.listener.onDownloadError, self.error)
        self.continue_event.set()

    async def cancel_download(self):
        self.is_cancelled = True
        await self.listener.onDownloadError("Download Canceled by user")


class AsyncExecutor:
    def __init__(self):
        self.continue_event = Event()

    def do(self, function, args):
        self.continue_event.clear()
        function(*args)
        self.continue_event.wait()


async def add_mega_download(mega_link: str, path: str, listener, name: Optional[str] = None):
    try:
        MEGA_EMAIL = config_dict.get('MEGA_EMAIL', '')
        MEGA_PASSWORD = config_dict.get('MEGA_PASSWORD', '')

        executor = AsyncExecutor()
        api = MegaApi(None, None, None, 'Z-Mirror')
        folder_api = None
        mega_listener = MegaAppListener(executor.continue_event, listener)
        api.addListener(mega_listener)

        # Cleanup function
        async def cleanup():
            try:
                await sync_to_async(executor.do, api.logout, ())
                if folder_api is not None:
                    await sync_to_async(executor.do, folder_api.logout, ())
            except Exception as e:
                LOGGER.error(f'Cleanup error: {e}')

        # Login to MEGA account if credentials provided
        if MEGA_EMAIL and MEGA_PASSWORD:
            await sync_to_async(executor.do, api.login, (MEGA_EMAIL, MEGA_PASSWORD))
            if mega_listener.error:
                raise Exception(f"Login error: {mega_listener.error}")

        # Handle public node or folder authorization
        link_type = get_mega_link_type(mega_link)
        if link_type == "file":
            await sync_to_async(executor.do, api.getPublicNode, (mega_link,))
            node = mega_listener.public_node
        else:
            folder_api = MegaApi(None, None, None, 'Z-Mirror')
            folder_api.addListener(mega_listener)
            await sync_to_async(executor.do, folder_api.loginToFolder, (mega_link,))
            node = await sync_to_async(folder_api.authorizeNode, mega_listener.node)

        if mega_listener.error:
            raise Exception(f"Mega error: {mega_listener.error}")

        name = name or node.getName()
        
        # Check for duplicates
        msg, button = await stop_duplicate_check(name, listener)
        if msg:
            await sendMessage(listener.message, msg, button)
            await cleanup()
            await delete_links(listener.message)
            return

        # Check size limits
        gid = token_urlsafe(6).replace('-', '')
        size = api.getSize(node)
        if limit_exceeded := await limit_checker(size, listener, isMega=True):
            await sendMessage(listener.message, limit_exceeded)
            await cleanup()
            await delete_links(listener.message)
            return

        # Queue handling
        added_to_queue, event = await is_queued(listener.uid)
        if added_to_queue:
            LOGGER.info(f"Added to Queue/Download: {name}")
            async with download_dict_lock:
                download_dict[listener.uid] = QueueStatus(
                    name, size, gid, listener, 'Dl')
            await listener.onDownloadStart()
            await sendStatusMessage(listener.message)
            await event.wait()
            async with download_dict_lock:
                if listener.uid not in download_dict:
                    await cleanup()
                    await delete_links(listener.message)
                    return
            from_queue = True
        else:
            from_queue = False

        # Start download
        async with download_dict_lock:
            download_dict[listener.uid] = MegaDownloadStatus(
                name, size, gid, mega_listener, listener.message, listener.extra_details)
        
        async with queue_dict_lock:
            non_queued_dl.add(listener.uid)

        if not from_queue:
            await listener.onDownloadStart()
            await sendStatusMessage(listener.message)
        
        LOGGER.info(f"Download from Mega: {name}")
        await makedirs(path, exist_ok=True)
        
        # Start the actual download
        await sync_to_async(executor.do, api.startDownload, (node, path, name, None, False, None))
        
        # Final cleanup
        await cleanup()

    except Exception as e:
        LOGGER.error(f'Mega download error: {e}')
        error_msg = f"Mega download error: {str(e)}"
        await listener.onDownloadError(error_msg)
        await delete_links(listener.message)

# Register cleanup at exit
atexit.register(lambda: LOGGER.info("Mega download handler shutdown"))
