"""Compress a video with ffmpeg and turn it into a base64 data URL for HTML embedding."""
import base64
import os
import subprocess
import tempfile


PRESETS = {
    'heavy':    (100, 48),
    'balanced': (150, 64),
    'light':    (280, 96),
}


def compress_for_embed(input_path: str, preset: str = 'balanced',
                       ffmpeg_env: dict = None) -> str:
    """Re-encode video at a small bitrate and return the path to the compressed mp4."""
    if preset not in PRESETS:
        raise ValueError(f"Unknown preset '{preset}'. Choose from: {list(PRESETS)}")

    v_kbps, a_kbps = PRESETS[preset]
    out_dir  = tempfile.mkdtemp(prefix='video_embed_')
    out_path = os.path.join(out_dir, 'embed.mp4')

    cmd = [
        'ffmpeg', '-y', '-i', input_path,
        '-c:v', 'libx264',
        '-preset', 'medium',
        '-b:v', f'{v_kbps}k',
        '-maxrate', f'{int(v_kbps * 1.5)}k',
        '-bufsize', f'{v_kbps * 2}k',
        '-vf', 'scale=-2:360',
        '-c:a', 'aac',
        '-b:a', f'{a_kbps}k',
        '-movflags', '+faststart',
        out_path,
    ]
    subprocess.run(cmd, check=True, capture_output=True, env=ffmpeg_env)
    return out_path


def to_data_url(path: str) -> str:
    """Read an mp4 and return it as a `data:video/mp4;base64,...` URL."""
    with open(path, 'rb') as f:
        encoded = base64.b64encode(f.read()).decode('ascii')
    return 'data:video/mp4;base64,' + encoded
