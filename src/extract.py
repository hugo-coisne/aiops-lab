import zipfile
import os.path
import shutil

# clear destination directory
dest_dir = 'extracted_data'
if os.path.exists(dest_dir):
  shutil.rmtree(dest_dir)

# get all zip files in zips directory
zips_path = "zips"
zip_list = os.listdir(zips_path)

# extract all zip files to destination directory
for i in zip_list:
  with zipfile.ZipFile(f'{zips_path}/{i}', 'r') as zip_ref:
    for j in zip_ref.namelist():
      if j[-4:]=='.log': # extract only .log files
        zip_ref.extract(j, dest_dir) # to destination dir

print('done')