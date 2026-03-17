#%%
import shutil
from pathlib import Path
from e2ude_core.services.fs_scanner import scan_for_rsm_zips

source_dir = Path(r"\\esidme24\#ESIDME24\PUBLIC\E2 Stuff\ALE RSM Data Archive")
dest_dir = Path(r"./fixtures")
MAX_FILES = 30
copied_count = 0

# for source_file in source_dir.rglob("*RSM*.zip"):
#     # Stop once we hit our sample limit
#     if copied_count >= MAX_FILES:
#         break
        
#     # Calculate relative path to maintain directory tree
#     relative_path = source_file.relative_to(source_dir)
#     target_file = dest_dir / relative_path
    
#     # Create parent directories
#     target_file.parent.mkdir(parents=True, exist_ok=True)
    
#     # Copy file
#     copied_count += 1
#     print(f"Copied {copied_count}/{MAX_FILES}: {relative_path}")

if __name__ == '__main__':
    res = scan_for_rsm_zips(source_dir)
    print(f"Found {len(res)} results")

# shutil.copy2(source_file, target_file)
# print("Finished sampling fixtures.")
# %%
