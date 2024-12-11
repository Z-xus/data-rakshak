import os

def preview_and_rename(directory):
    """Preview and rename files and update content with confirmation"""
    
    # File content replacements
    replacements = {
        'presidio_analyzer': 'guardian_analyzer',
        'presidio-analyzer': 'guardian-analyzer',
        'PresidioPDFRedactor': 'GuardianPDFRedactor',
        'Presidio analysis': 'Guardian analysis',
        'presidio.': 'guardian.',
        '"presidio-': '"guardian-',
        "'presidio-": "'guardian-",
    }
    
    changes = []

    # Walk through directory
    for root, dirs, files in os.walk(directory):
        # Preview directory renames
        for dir_name in dirs:
            if 'presidio' in dir_name.lower():
                new_name = dir_name.replace('presidio', 'guardian')
                old_path = os.path.join(root, dir_name)
                new_path = os.path.join(root, new_name)
                changes.append((old_path, new_path))
        
        # Preview file renames and content changes
        for file_name in files:
            if file_name.endswith(('.py', '.md', '.txt', '.yml', '.yaml')):
                file_path = os.path.join(root, file_name)
                
                # Read content
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Check for content changes
                new_content = content
                for old, new in replacements.items():
                    new_content = new_content.replace(old, new)
                
                if new_content != content:
                    changes.append((file_path, "Content changes"))
                
                # Check for file rename
                if 'presidio' in file_name.lower():
                    new_file_name = file_name.replace('presidio', 'guardian')
                    new_file_path = os.path.join(root, new_file_name)
                    changes.append((file_path, new_file_path))

    # Display changes
    print("Proposed changes:")
    for old, new in changes:
        print(f"Rename/Change: {old} -> {new}")

    # Confirm changes
    confirm = input("\nApply these changes? (y/n): ").strip().lower()
    if confirm == 'y':
        for old, new in changes:
            if new == "Content changes":
                # Apply content changes
                with open(old, 'r', encoding='utf-8') as f:
                    content = f.read()
                new_content = content
                for old_str, new_str in replacements.items():
                    new_content = new_content.replace(old_str, new_str)
                with open(old, 'w', encoding='utf-8') as f:
                    f.write(new_content)
            else:
                # Apply renames
                os.rename(old, new)
        print("Changes applied.")
    else:
        print("No changes made.")

if __name__ == '__main__':
    # Run from project root
    preview_and_rename('./guardian-analyzer') 