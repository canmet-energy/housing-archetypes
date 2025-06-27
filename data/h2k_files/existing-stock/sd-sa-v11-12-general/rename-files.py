import os

def prepend_top_level_folder_name(parent_dir):
    for top_level in os.listdir(parent_dir):
        top_level_path = os.path.join(parent_dir, top_level)
        if os.path.isdir(top_level_path):
            for root, dirs, files in os.walk(top_level_path):
                for filename in files:
                    file_path = os.path.join(root, filename)
                    name, ext = os.path.splitext(filename)

                    # Skip if already starts with folder name
                    if name.startswith(f"{top_level}_"):
                        continue

                    new_name = f"{top_level}_{name}{ext}"
                    new_path = os.path.join(root, new_name)
                    os.rename(file_path, new_path)
                    print(f"Renamed: {file_path} -> {new_path}")

# Example usage:
parent_directory = r"C:\Users\jpurdy\OneDrive - NRCan RNCan\f280-hpxml\output"
prepend_top_level_folder_name(parent_directory)
