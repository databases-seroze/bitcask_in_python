
# Could be run in a background thread 

"""
Compactor(directory, keydir)
compact(immutable_files) → (new_merged_file, deleted_file_ids)
Does the copy-if-live loop
Writes the merged data file + hint file
Updates keydir in place
Deletes old files
"""

