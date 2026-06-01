"""Robust Telegram video-downloader bot.

This file has been refactored for production use:
- No hard-coded tokens: requires TELEGRAM_TOKEN environment variable.
- Async-safe download using thread executor to avoid blocking the event loop.
- Per-download temporary directory to avoid filename collisions.
- Concurrency limit to prevent resource exhaustion.
- Input validation and detailed exception handling.
- Rotating file logging for diagnostics.
"""

from __future__ import annotations

import asyncio
import logging
from logging.handlers import RotatingFileHandler
import os
import shutil
import tempfile
from pathlib import Path
import threading
import uuid
from typing import Optional

import yt_dlp
from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


def _load_token_from_dotenv() -> Optional[str]:
    """Load BOT_TOKEN or TELEGRAM_TOKEN from a local .env file if present."""
    dotenv_path = Path(".env")
    if not dotenv_path.exists():
        return None

    try:
        for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")

            if key in {"BOT_TOKEN", "TELEGRAM_TOKEN"} and value:
                return value
    except OSError:
        return None

    return None


# Production: require the token via environment variable. Do NOT hardcode tokens.
TOKEN = os.environ.get("BOT_TOKEN") or os.environ.get("TELEGRAM_TOKEN")
if not TOKEN:
    TOKEN = _load_token_from_dotenv()
if not TOKEN:
    raise RuntimeError(
        "A Telegram token is required. Set BOT_TOKEN or TELEGRAM_TOKEN, "
        "or add BOT_TOKEN=... or TELEGRAM_TOKEN=... to a local .env file."
    )

# Basic structured logging with rotation to avoid unbounded disk usage.
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
logger = logging.getLogger("telegram_bot")
if not logger.handlers:
    handler = RotatingFileHandler(LOG_DIR / "bot.log", maxBytes=5 * 1024 * 1024, backupCount=3)
    fmt = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    # Also log to stdout for container logs.
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)
    logger.setLevel(logging.INFO)

# Concurrency controls: avoid simultaneous downloads overwhelming the host.
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "2"))
_download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# Maximum allowed file size in bytes (default 50 MB). Can be configured
# via the environment variable MAX_FILE_SIZE (in bytes) or keep default.
MAX_FILE_SIZE = int(os.environ.get("MAX_FILE_SIZE", str(50 * 1024 * 1024)))

# Track pending requests for callback handling and cancellations.
# Keys are request ids (uuid4 strings).
_pending_requests: dict[str, dict] = {}


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a friendly welcome message.

    Kept minimal for privacy. This handler is intentionally simple and
    non-blocking.
    """
    await update.message.reply_text(
        "👋 مرحباً!\n\n" "أرسل رابط الفيديو وسأحاول تحميله وإرساله لك."
    )


async def _prompt_actions(update: Update, url: str) -> str:
    """Send inline buttons for download options and return request id."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    req_id = str(uuid.uuid4())
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Download Video", callback_data=f"dl:video:{req_id}"),
                InlineKeyboardButton("Download MP3", callback_data=f"dl:audio:{req_id}"),
            ],
            [InlineKeyboardButton("Cancel", callback_data=f"dl:cancel:{req_id}")],
        ]
    )

    sent = await update.message.reply_text(
        "اختر إجراء:", reply_markup=kb
    )

    # store minimal request metadata
    _pending_requests[req_id] = {
        "url": url,
        "chat_id": update.effective_chat.id,
        "message_id": sent.message_id,
        "user_id": update.effective_user.id if update.effective_user else None,
        "cancel_event": threading.Event(),
        "task": None,
    }

    logger.info("Prompted actions for request %s by user %s", req_id, update.effective_user.id if update.effective_user else None)
    return req_id


def _is_valid_url(url: str) -> bool:
    """Perform minimal URL validation.

    We avoid heavy whitelisting here because yt-dlp supports many hosts.
    The check ensures a scheme exists and the length is reasonable.
    """
    try:
        from urllib.parse import urlparse

        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return False
        if not p.netloc:
            return False
        if len(url) > 2048:
            return False
        return True
    except Exception:
        return False


def _run_yt_dlp(
    url: str,
    outtmpl: str,
    cancel_event: Optional[threading.Event] = None,
    postprocessors: Optional[list] = None,
    ydl_format: Optional[str] = None,
) -> str:
    """Synchronous helper executed in a thread to download the video.

    Returns the full path to the downloaded file. Raises exceptions from
    yt_dlp on failure which are handled by the caller.
    """
    # yt_dlp is blocking and CPU-light; run it in a thread from asyncio.
    # Build options including an optional progress hook that checks for
    # cancellation.
    def _progress(d):
        # d is a dict provided by yt-dlp with status info.
        if cancel_event and cancel_event.is_set():
            raise yt_dlp.utils.DownloadError("Download cancelled by user")

    ydl_opts = {
        "format": ydl_format or "bestvideo+bestaudio/best",
        "outtmpl": outtmpl,
        "noplaylist": True,
        "no_warnings": True,
        "quiet": True,
        "retries": 3,
        "progress_hooks": [_progress],
    }

    if postprocessors:
        ydl_opts["postprocessors"] = postprocessors

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)

        # yt-dlp may run postprocessors (FFmpeg) that change the final
        # filename/extension. The safest approach is to scan the output
        # directory and pick the largest file produced for this request.
        out_dir = Path(outtmpl).parent
        candidates = [p for p in out_dir.iterdir() if p.is_file()]
        if not candidates:
            # Fall back to prepare_filename if nothing found (rare).
            filename = ydl.prepare_filename(info)
            return filename

        # Return the largest file which is likely the final media file.
        largest = max(candidates, key=lambda p: p.stat().st_size)
        return str(largest)


def _estimate_size(url: str) -> Optional[int]:
    """Estimate download size in bytes using yt-dlp metadata (no download).

    Returns filesize in bytes if available, otherwise None.
    """
    try:
        ydl_opts = {"quiet": True, "no_warnings": True}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        # Prefer overall filesize if present
        size = info.get("filesize") or info.get("filesize_approx")
        if size:
            return int(size)

        # Otherwise, inspect formats and take the best available estimate
        formats = info.get("formats") or []
        max_size = 0
        for f in formats:
            fs = f.get("filesize") or f.get("filesize_approx")
            if fs:
                try:
                    max_size = max(max_size, int(fs))
                except Exception:
                    continue
        return max_size or None
    except Exception as ex:
        logger.debug("Size estimation failed for %s: %s", url, ex)
        return None


async def _download_and_send(
    target,
    url: str,
    tmp_dir: Path,
    postprocessors: Optional[list] = None,
    ydl_format: Optional[str] = None,
) -> None:
    """Download with yt-dlp in a thread and send to user.

    This isolates blocking operations to threads and ensures cleanup.
    """
    # Build an output template inside our temp directory to avoid
    # collisions across concurrent downloads.
    outtmpl = str(tmp_dir / "%(id)s.%(ext)s")

    loop = asyncio.get_running_loop()

    # Find pending meta and its cancel_event if present.
    req = None
    for rid, meta in _pending_requests.items():
        if meta.get("tmp_dir") == str(tmp_dir):
            req = meta
            break

    cancel_event = req.get("cancel_event") if req else None

    filename: Optional[str] = None
    try:
        # Execute blocking download in a threadpool
        filename = await loop.run_in_executor(
            None, _run_yt_dlp, url, outtmpl, cancel_event, postprocessors, ydl_format
        )

        if not filename or not Path(filename).exists():
            raise FileNotFoundError("Downloaded file not found")

        file_size = Path(filename).stat().st_size

        # Choose sending method based on size. Telegram has limits for
        # previewable videos; large files are safer to send as documents.
        MAX_VIDEO_PREVIEW = 50 * 1024 * 1024  # 50 MB

        # target can be an Update or a Message; normalize to a message-like object
        if hasattr(target, "message") and target.message:
            message_obj = target.message
        else:
            message_obj = target

        with open(filename, "rb") as fh:
            if file_size <= MAX_VIDEO_PREVIEW and ydl_format and "video" in ydl_format:
                await message_obj.reply_video(video=fh, caption="✅ تم التحميل بنجاح")
            else:
                await message_obj.reply_document(document=fh, caption="✅ تم التحميل (ملف كبير)")

        chat_id = message_obj.chat.id if hasattr(message_obj, "chat") else None
        logger.info("Sent file %s to chat %s", filename, chat_id)
    except Exception as ex:
        logger.exception("Error while downloading/sending file: %s", ex)
        # Re-raise to allow upstream handlers to react if needed
        raise
    finally:
        # Best-effort cleanup. Remove the temp dir and contents.
        try:
            shutil.rmtree(tmp_dir)
        except Exception as ex:
            logger.warning("Failed to remove temp dir %s: %s", tmp_dir, ex)


async def download_video(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for incoming messages with URLs to download.

    This function validates input, enforces concurrency limits, provides
    user feedback, and traps errors so the bot doesn't crash.
    """
    text = (update.message.text or "").strip()
    if not text:
        await update.message.reply_text("❌ لم يتم العثور على نص في الرسالة.")
        return

    # Basic URL validation to avoid passing arbitrary text to yt-dlp.
    if not _is_valid_url(text):
        await update.message.reply_text("❌ الرجاء إرسال رابط صحيح (http/https).")
        return

    # Show action buttons and store request metadata. Actual download
    # starts when the user presses a button.
    await _prompt_actions(update, text)


async def health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Simple health check for operators to verify the bot is alive."""
    await update.message.reply_text("OK")


async def _handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Process inline button presses: download video, download audio, cancel."""
    cq = update.callback_query
    if not cq or not cq.data:
        return
    await cq.answer()

    parts = cq.data.split(":")
    if len(parts) != 3 or parts[0] != "dl":
        logger.debug("Unknown callback data: %s", cq.data)
        return

    action = parts[1]
    req_id = parts[2]
    meta = _pending_requests.get(req_id)
    if not meta:
        await cq.message.edit_text("⚠️ هذا الطلب غير موجود أو تم الانتهاء منه.")
        return

    url = meta.get("url")
    chat_id = meta.get("chat_id")

    if action == "cancel":
        meta["cancel_event"].set()
        task = meta.get("task")
        if task and not task.done():
            try:
                task.cancel()
            except Exception:
                pass
        await cq.message.edit_text("❌ تم إلغاء الطلب.")
        logger.info("Request %s cancelled by user %s", req_id, meta.get("user_id"))
        _pending_requests.pop(req_id, None)
        return

    # For download actions: check size and start download task.
    if action in ("video", "audio"):
        # Prevent double-processing
        if meta.get("processing"):
            await cq.message.edit_text("⚠️ الطلب قيد المعالجة بالفعل...")
            return
        meta["processing"] = True

        # Estimate size before starting download
        await cq.message.edit_text("🔎 جارٍ التحقق من حجم الملف...")
        est = _estimate_size(url)
        if est is not None and est > MAX_FILE_SIZE:
            await context.bot.send_message(chat_id=chat_id, text=f"❌ حجم الملف يساوي {est/(1024*1024):.2f} MB، وهو أكبر من الحد {MAX_FILE_SIZE/(1024*1024):.2f} MB.")
            logger.info("Rejected request %s due to size %s", req_id, est)
            _pending_requests.pop(req_id, None)
            try:
                await cq.message.delete()
            except Exception:
                pass
            return

        # Acquire semaphore to limit concurrent downloads
        try:
            await asyncio.wait_for(_download_semaphore.acquire(), timeout=5.0)
        except asyncio.TimeoutError:
            await context.bot.send_message(chat_id=chat_id, text="⚠️ الخوادم مشغولة حالياً، حاول مرة أخرى لاحقاً.")
            meta["processing"] = False
            return

        # Prepare temporary directory and task
        tmp_dir = Path(tempfile.mkdtemp(prefix=f"tgdl_{req_id}_"))
        meta["tmp_dir"] = str(tmp_dir)

        # Edit prompt message to show downloading status
        try:
            await cq.message.edit_text("⏳ جاري التنزيل...")
        except Exception:
            pass

        # Decide ydl options per action
        if action == "audio":
            postprocessors = [
                {"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}
            ]
            ydl_format = "bestaudio/best"
        else:
            postprocessors = None
            ydl_format = "bestvideo+bestaudio/best"

        async def _run():
            try:
                await _download_and_send(cq.message.reply_to_message or cq.message, url, tmp_dir, postprocessors=postprocessors, ydl_format=ydl_format)
                try:
                    await context.bot.send_message(chat_id=chat_id, text="✅ تم الإرسال بنجاح.")
                except Exception:
                    pass
                logger.info("Completed request %s for user %s", req_id, meta.get("user_id"))
            except yt_dlp.utils.DownloadError as de:
                logger.exception("Download failed for %s: %s", req_id, de)
                try:
                    await context.bot.send_message(chat_id=chat_id, text="❌ فشل التحميل أو تم الإلغاء.")
                except Exception:
                    pass
            except Exception as ex:
                logger.exception("Unexpected error for %s: %s", req_id, ex)
                try:
                    await context.bot.send_message(chat_id=chat_id, text="❌ حدث خطأ غير متوقع أثناء المعالجة.")
                except Exception:
                    pass
            finally:
                # Cleanup and release semaphore
                try:
                    shutil.rmtree(tmp_dir)
                except Exception:
                    pass
                _download_semaphore.release()
                _pending_requests.pop(req_id, None)

        task = asyncio.create_task(_run())
        meta["task"] = task
        logger.info("Started download task %s for user %s", req_id, meta.get("user_id"))
        return


def build_app() -> Application:
    """Create and configure the Application instance.

    Keeping app construction separate eases testing and potential reuse.
    """
    app = Application.builder().token(TOKEN).build()

    # Command handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("health", health))
    # Callback handler for inline buttons
    app.add_handler(CallbackQueryHandler(_handle_callback))

    # Message handler: accepts text messages that are not commands.
    app.add_handler(MessageHandler((filters.TEXT & (~filters.COMMAND)), download_video))

    return app


def main() -> None:
    """Entry point: start polling. The bot will run until interrupted.

    For long-running deployments prefer running under a process supervisor
    (systemd, supervisor, Docker restart policy) so it restarts automatically
    on crashes or host reboots.
    """
    app = build_app()
    logger.info("Starting bot")
    app.run_polling()


if __name__ == "__main__":
    main()
