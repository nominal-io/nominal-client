# Nominal Video CLI Examples

The `nom video` command provides powerful video processing capabilities with GPU acceleration.

## Commands Overview

- `nom video convert` - Convert videos with GPU acceleration
- `nom video check-gpu` - Check available GPU acceleration options
- `nom video info` - Get detailed video file information  
- `nom video presets` - List encoding presets for different GPU types

## Basic Usage Examples

### 1. Check GPU Acceleration Support
```powershell
# Check what GPU acceleration is available
nom video check-gpu
```

### 2. Convert Video with Auto GPU Detection
```powershell
# Convert with automatic GPU detection and letterboxing
nom video convert -i input.mp4 -o output.mp4 --resolution 1080p

# Convert vertical GoPro video to horizontal with letterboxing
nom video convert -i "Flight 13 GoPro Original.MP4" -o flight_converted.mp4 --resolution 1080p
```

### 3. Custom Resolution with Letterboxing
```powershell
# Convert to custom resolution maintaining aspect ratio
nom video convert -i vertical_video.mp4 -o horizontal_output.mp4 --width 1920 --height 1080

# Convert without letterboxing (stretches video)
nom video convert -i input.mp4 -o output.mp4 --resolution 1080p --stretch
```

### 4. GPU-Specific Encoding
```powershell
# Use NVIDIA GPU with fast preset
nom video convert -i input.mp4 -o output.mp4 --gpu nvidia --preset fast

# Use Intel Quick Sync with high quality
nom video convert -i input.mp4 -o output.mp4 --gpu intel --preset slow

# Disable GPU acceleration (CPU only)
nom video convert -i input.mp4 -o output.mp4 --gpu none
```

### 5. Advanced Options
```powershell
# Custom keyframe interval for streaming
nom video convert -i input.mp4 -o output.mp4 --keyframe-interval 1

# Don't overwrite existing files
nom video convert -i input.mp4 -o output.mp4 --no-force
```

### 6. Video Information
```powershell
# Get detailed video information
nom video info "my_video.mp4"

# Example output:
# 📹 Video Information:
#    Resolution: 3840x2160
#    Codec: hevc
#    Frame rate: 29.97
#    Duration: 2114.11s
#    Rotation: 90°
#    Frame count: 63,412
# 🔊 Audio Information:
#    Track 1: aac, 2 channels, 48000 Hz
```

### 7. List Available Presets
```powershell
# See all encoding presets for each GPU type
nom video presets
```

## Common Workflows

### GoPro Video Processing
```powershell
# Step 1: Check the video info
nom video info "GoPro_Video.MP4"

# Step 2: Convert with letterboxing for horizontal playback
nom video convert -i "GoPro_Video.MP4" -o "GoPro_Converted.mp4" --resolution 1080p --gpu auto --preset fast
```

### Batch Processing (PowerShell)
```powershell
# Convert all MP4 files in a directory
Get-ChildItem "*.mp4" | ForEach-Object {
    $output = $_.BaseName + "_converted.mp4"
    nom video convert -i $_.Name -o $output --resolution 1080p --gpu auto
}
```

### High Quality Conversion
```powershell
# Use slower preset for better quality
nom video convert -i input.mp4 -o output.mp4 --resolution 1080p --gpu nvidia --preset slow

# Custom resolution for specific use case
nom video convert -i input.mp4 -o output.mp4 --width 1280 --height 720 --gpu auto --preset medium
```

## Performance Tips

1. **GPU Acceleration**: Always use `--gpu auto` unless you need a specific GPU
2. **Presets**: Use `fast` for quick processing, `medium` for balanced, `slow` for quality
3. **Resolution**: Letterboxing preserves quality better than stretching
4. **Keyframes**: Use shorter intervals (1-2s) for streaming, longer for storage

## Troubleshooting

### No GPU Acceleration Available
```powershell
# Check if ffmpeg supports GPU
nom video check-gpu

# If no GPU support, install proper ffmpeg build:
# - Windows: Use ffmpeg with NVENC/AMF/QSV support
# - macOS: Use ffmpeg with VideoToolbox support  
# - Linux: Install gpu-specific codec packages
```

### Large File Sizes
```powershell
# Use more aggressive compression
nom video convert -i input.mp4 -o output.mp4 --resolution 720p --gpu auto --preset fast

# Check file sizes before/after
nom video info input.mp4
nom video info output.mp4
``` 