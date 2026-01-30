import asyncio
import hashlib
import logging
import os
import shutil
import time
from typing import Dict, Optional

from mediaflow_proxy.configs import settings

logger = logging.getLogger(__name__)

class FFmpegManager:
    """
    Manages FFmpeg processes for transcoding MPD streams to HLS (MPEG-TS).
    Adapted from EasyProxy for mediaflow-proxy.
    """
    def __init__(self, temp_dir: str = None):
        self.temp_dir = temp_dir or settings.ffmpeg_temp_dir
        self.processes: Dict[str, asyncio.subprocess.Process] = {}
        self.access_times: Dict[str, float] = {}
        self.active_streams: Dict[str, str] = {} # stream_id -> original_url
        
        # Ensure temp directory exists
        os.makedirs(self.temp_dir, exist_ok=True)

    async def get_stream(self, url: str, headers: dict = None, clearkey: str = None) -> Optional[str]:
        """
        Starts (or returns existing) FFmpeg stream for the URL.
        Returns the stream_id.
        """
        # Include clearkey in hash to invalidate cache if key changes
        unique_str = f"{url}|{clearkey}" if clearkey else url
        stream_id = hashlib.md5(unique_str.encode()).hexdigest()
        
        stream_dir = os.path.join(self.temp_dir, stream_id)
        playlist_path = os.path.join(stream_dir, "index.m3u8")
        
        self.access_times[stream_id] = time.time()
        
        if stream_id in self.processes:
            # Check if process is still running
            proc = self.processes[stream_id]
            if proc.returncode is None:
                # Process is running, check if playlist exists
                if os.path.exists(playlist_path):
                    return stream_id
                
                # File not found but process is running. It might be initializing.
                logger.info(f"Stream {stream_id} is initializing. Waiting for playlist...")
                for _ in range(100): # Wait up to 10s
                    if os.path.exists(playlist_path):
                        return stream_id
                    if proc.returncode is not None:
                         # Process died while waiting
                         logger.warning(f"Process {stream_id} died while waiting.")
                         del self.processes[stream_id]
                         break
                    await asyncio.sleep(0.1)
                
                # If still (running) and no file -> Stale/Stuck?
                if proc.returncode is None and not os.path.exists(playlist_path):
                     logger.warning(f"Stream {stream_id} timed out initializing. Restarting.")
                     try:
                        proc.kill()
                     except: pass
                     del self.processes[stream_id]

            else:
                logger.warning(f"FFmpeg process for {stream_id} exited with {proc.returncode}. Restarting.")
                del self.processes[stream_id]
        
        # Start new stream
        return await self._start_ffmpeg(url, headers, stream_id, clearkey)

    async def _start_ffmpeg(self, url: str, headers: dict, stream_id: str, clearkey: str = None) -> Optional[str]:
        stream_dir = os.path.join(self.temp_dir, stream_id)
        
        # Clean existing dir if any
        if os.path.exists(stream_dir):
            try:
                shutil.rmtree(stream_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Error cleaning stream dir {stream_dir}: {e}")
        
        os.makedirs(stream_dir, exist_ok=True)
        
        playlist_path = os.path.join(stream_dir, "index.m3u8")
        ffmpeg_log_path = os.path.join(stream_dir, "ffmpeg.log")
        
        # Build command
        headers_str = ""
        if headers:
            # Filter headers that might cause issues with FFmpeg
            valid_headers = {k: v for k, v in headers.items() if k.lower() not in ['host', 'connection', 'accept-encoding']}
            headers_str = "\r\n".join([f"{k}: {v}" for k, v in valid_headers.items()])
        
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "warning",
            # --- CRITICAL: Timestamp and sync fixes ---
            "-fflags", "+genpts+discardcorrupt+igndts",
            "-analyzeduration", "10000000",
            "-probesize", "10000000",
            # --- Network resilience ---
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_delay_max", "5",
        ]

        if headers_str:
            cmd.extend(["-headers", headers_str])
        
        # Decryption Key handling - supports multi-key format "KID1:KEY1,KID2:KEY2"
        if clearkey:
            try:
                keys_to_use = []
                if ':' in clearkey:
                     pairs = clearkey.split(',')
                     for pair in pairs:
                         if ':' in pair:
                             _, key = pair.split(':')
                             keys_to_use.append(key.strip())
                else:
                    keys_to_use.append(clearkey)
                
                for key in keys_to_use:
                    cmd.extend(["-cenc_decryption_key", key])
                
                if keys_to_use:
                    logger.info(f"Added {len(keys_to_use)} decryption key(s) to FFmpeg command")
            except Exception as e:
                logger.error(f"Error parsing clearkey: {e}")
        
        # Use wallclock timestamps to prevent jumps due to source timestamp resets/loops
        cmd.extend(["-use_wallclock_as_timestamps", "1"])
        cmd.extend(["-i", url])

        # Explicit mapping to ensure video is selected
        cmd.extend(["-map", "0:v?", "-map", "0:a?"])

        # Stream copy (faster, less CPU, but less compatible)
        cmd.extend([
            "-c", "copy",
            "-ignore_unknown",
            "-vsync", "passthrough", # Do not drop/duplicate frames
        ])

        # Bitstream filter determines compatibility for HLS/MPEG-TS
        # We apply h264_mp4toannexb by default for H.264 streams
        # But we need to be careful if the source is HEVC or others
        cmd.extend([
            "-bsf:v", "h264_mp4toannexb",
            "-avoid_negative_ts", "make_zero",
            "-max_muxing_queue_size", "2048",
            "-f", "hls",
            "-hls_time", "2",
            "-hls_list_size", "15",
            "-hls_flags", "delete_segments+independent_segments",
            "-hls_segment_filename", os.path.join(stream_dir, "segment_%03d.ts"),
            playlist_path
        ])
        
        logger.info(f"Starting FFmpeg for {stream_id}")
        
        try:
            # Write command to log for debugging
            with open(ffmpeg_log_path, "w") as log_file:
                log_file.write(f"Command: {' '.join(cmd)}\n\n")
            
            # Open log file in append mode for stderr/stdout
            log_handle = open(ffmpeg_log_path, "a")

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=log_handle,
                stderr=log_handle
            )

            self.processes[stream_id] = process
            self.active_streams[stream_id] = url
            
            # Wait for the playlist to appear (up to 30 seconds)
            for _ in range(300):
                if os.path.exists(playlist_path):
                    log_handle.close()
                    break
                # Check if process died
                if process.returncode is not None:
                    log_handle.close()
                    with open(ffmpeg_log_path, "r") as f:
                        log_content = f.read()
                    logger.error(f"FFmpeg process died. Log tail: {log_content[-500:]}")
                    return None
                await asyncio.sleep(0.1)
            else:
                log_handle.close()
            
            if not os.path.exists(playlist_path):
                 logger.error("Timeout waiting for playlist generation")
                 try:
                     process.terminate()
                 except: pass
                 return None
                
            return stream_id
            
        except Exception as e:
            logger.error(f"Failed to start FFmpeg: {e}")
            return None

    async def cleanup_loop(self):
        """Periodically checks and terminates idle streams."""
        while True:
            try:
                now = time.time()
                to_remove = []
                
                for stream_id, last_access in list(self.access_times.items()):
                    # Timeout after settings.ffmpeg_idle_timeout seconds of inactivity
                    if now - last_access > settings.ffmpeg_idle_timeout:
                        logger.info(f"Stream {stream_id} idle for {settings.ffmpeg_idle_timeout}s. Terminating.")
                        to_remove.append(stream_id)
                
                for stream_id in to_remove:
                    await self._stop_stream(stream_id)
                    
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
            
            await asyncio.sleep(10)

    async def _stop_stream(self, stream_id: str):
        if stream_id in self.processes:
            proc = self.processes[stream_id]
            try:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    proc.kill()
            except Exception as e:
                logger.error(f"Error killing process {stream_id}: {e}")
            del self.processes[stream_id]
            
        if stream_id in self.access_times:
            del self.access_times[stream_id]
        if stream_id in self.active_streams:
            del self.active_streams[stream_id]
            
        # Clean disk
        stream_dir = os.path.join(self.temp_dir, stream_id)
        if os.path.exists(stream_dir):
            try:
                shutil.rmtree(stream_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Error removing stream dir {stream_dir}: {e}")

    def touch_stream(self, stream_id: str):
        """Updates last access time for a stream."""
        if stream_id in self.access_times:
            self.access_times[stream_id] = time.time()

ffmpeg_manager = FFmpegManager()
