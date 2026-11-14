import subprocess
import tempfile
from pathlib import Path


def finalize_result_video(source: Path) -> Path:
    import imageio_ffmpeg

    options = {"creationflags": subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS} if hasattr(subprocess, "CREATE_NO_WINDOW") else {}
    with tempfile.TemporaryDirectory(prefix=".result_encoding_", dir=source.parent) as directory:
        temporary = Path(directory) / "result.mp4"
        command = [imageio_ffmpeg.get_ffmpeg_exe(), "-hide_banner", "-loglevel", "error", "-nostdin",
                   "-threads", "1", "-i", str(source), "-map", "0:v:0", "-an",
                   "-c:v", "libx264", "-preset", "fast", "-crf", "18", "-pix_fmt", "yuv420p",
                   "-threads", "1", "-filter_threads", "1",
                   "-movflags", "+faststart", str(temporary)]
        result = subprocess.run(command, capture_output=True, text=True, **options)
        if result.returncode:
            raise RuntimeError(f"Result encoding failed; original result preserved: {source}\n{result.stderr[-2000:]}")
        temporary.replace(source)
    return source
