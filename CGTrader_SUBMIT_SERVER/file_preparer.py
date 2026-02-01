"""
File preparation module for CGTrader batch upload.
Extracts preview images to root and archives subfolders.
"""
import os
import shutil
import zipfile
from pathlib import Path
from typing import List, Tuple


def find_preview_images(root_path: str) -> List[Path]:
    """
    Recursively find all preview images (files with '_view' in name).
    
    Args:
        root_path: Root directory to search
        
    Returns:
        List of Path objects to preview images
    """
    root = Path(root_path)
    preview_images = []
    
    # Common preview image extensions
    image_extensions = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}
    
    for file_path in root.rglob('*'):
        if file_path.is_file():
            # Check if filename contains '_view' (case insensitive)
            if '_view' in file_path.name.lower():
                # Check if it's an image
                if file_path.suffix.lower() in image_extensions:
                    preview_images.append(file_path)
    
    return preview_images


def prepare_files_for_batch_upload(extract_path: str, output_path: str) -> str:
    """
    Prepare files for CGTrader batch upload:
    1. Find all *_view.* images and move them to root
    2. Archive each subfolder into separate .zip files
    
    Args:
        extract_path: Path to extracted ZIP directory
        output_path: Path where prepared files will be placed
        
    Returns:
        Path to the prepared folder
    """
    extract_dir = Path(extract_path)
    
    if not extract_dir.exists():
        raise FileNotFoundError(f"Extract path does not exist: {extract_path}")
    
    # Create output directory
    output_dir = Path(output_path)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"[FilePrep] Preparing files from {extract_path} to {output_path}")
    
    # Step 1: Find and copy all preview images to root
    preview_images = find_preview_images(extract_path)
    print(f"[FilePrep] Found {len(preview_images)} preview image(s)")
    
    copied_count = 0
    for preview_path in preview_images:
        # Get relative path to calculate target name
        try:
            rel_path = preview_path.relative_to(extract_dir)
            
            # Create unique filename if duplicates exist
            target_name = preview_path.name
            target_path = output_dir / target_name
            
            # Handle duplicates by adding parent folder name
            counter = 1
            while target_path.exists():
                stem = preview_path.stem
                suffix = preview_path.suffix
                target_name = f"{stem}_{counter}{suffix}"
                target_path = output_dir / target_name
                counter += 1
            
            # Copy file
            shutil.copy2(preview_path, target_path)
            copied_count += 1
            print(f"[FilePrep] Copied preview: {preview_path.name} -> {target_name}")
            
        except Exception as e:
            print(f"[FilePrep] Error copying {preview_path}: {e}")
    
    print(f"[FilePrep] Copied {copied_count} preview images to root")
    
    # Step 2: Archive each subfolder
    # Get immediate subdirectories (not files)
    subdirs = [d for d in extract_dir.iterdir() if d.is_dir()]
    
    archived_count = 0
    for subdir in subdirs:
        # Skip hidden directories
        if subdir.name.startswith('.'):
            continue
        
        # Create zip archive name from subdirectory name
        zip_name = f"{subdir.name}.zip"
        zip_path = output_dir / zip_name
        
        print(f"[FilePrep] Archiving {subdir.name} -> {zip_name}")
        
        try:
            # Create ZIP archive
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Add all files from subdirectory
                for file_path in subdir.rglob('*'):
                    if file_path.is_file():
                        # Calculate archive name (relative to subdirectory)
                        arcname = file_path.relative_to(subdir)
                        zipf.write(file_path, arcname)
                        
                # Get file count for logging
                file_count = sum(1 for _ in subdir.rglob('*') if _.is_file())
            
            # Check zip was created and has content
            zip_size = zip_path.stat().st_size
            print(f"[FilePrep] Archived {subdir.name}: {file_count} files, {zip_size} bytes")
            archived_count += 1
            
        except Exception as e:
            print(f"[FilePrep] Error archiving {subdir.name}: {e}")
            # Remove incomplete zip
            if zip_path.exists():
                zip_path.unlink()
    
    print(f"[FilePrep] Archived {archived_count} subdirectory(ies)")
    
    # Verify output directory has content
    output_files = list(output_dir.iterdir())
    file_count = len([f for f in output_files if f.is_file()])
    dir_count = len([d for d in output_files if d.is_dir()])
    
    print(f"[FilePrep] Prepared folder contains: {file_count} file(s), {dir_count} directory(ies)")
    
    if file_count == 0:
        raise ValueError(f"No files prepared in output directory: {output_path}")
    
    return str(output_dir)


def verify_prepared_folder(folder_path: str) -> Tuple[bool, str]:
    """
    Verify that prepared folder has correct structure.
    
    Args:
        folder_path: Path to prepared folder
        
    Returns:
        (is_valid, message)
    """
    folder = Path(folder_path)
    
    if not folder.exists():
        return False, f"Folder does not exist: {folder_path}"
    
    if not folder.is_dir():
        return False, f"Path is not a directory: {folder_path}"
    
    # Check for at least one file
    files = [f for f in folder.iterdir() if f.is_file()]
    if len(files) == 0:
        return False, "No files in prepared folder"
    
    # Count preview images
    preview_count = sum(1 for f in files if '_view' in f.name.lower())
    
    # Count zip files
    zip_count = sum(1 for f in files if f.suffix.lower() == '.zip')
    
    message = f"Valid: {preview_count} preview(s), {zip_count} archive(s), {len(files)} total file(s)"
    
    return True, message


if __name__ == "__main__":
    # Test script
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: file_preparer.py <extract_path> <output_path>")
        sys.exit(1)
    
    extract_path = sys.argv[1]
    output_path = sys.argv[2]
    
    try:
        prepared_path = prepare_files_for_batch_upload(extract_path, output_path)
        print(f"\n[FilePrep] Success! Prepared folder: {prepared_path}")
        
        is_valid, message = verify_prepared_folder(prepared_path)
        print(f"[FilePrep] Verification: {message}")
        
    except Exception as e:
        print(f"[FilePrep] Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
