def dl():
  from urllib.request import urlretrieve
  import os
  dest_dir = '../zips'
  if not os.path.exists(dest_dir):
    os.mkdir(dest_dir)
  dest_dir+='/'
  zips = [{'url':'https://zenodo.org/records/8196385/files/HDFS_v1.zip?download=1', 'filename':'HDFS_v1.zip'},
          {'url':'https://zenodo.org/records/8196385/files/BGL.zip?download=1', 'filename':'BGL.zip'}]
  for zip in zips:
    urlretrieve(zip['url'], f"{dest_dir}{zip['filename']}")