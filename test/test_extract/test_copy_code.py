"""
Not a test - just a cheating way of ensuring the contents of  
./layer_utils/python/utils directory are updating with changes to the 
./utils directory
should run on every invocation of the testing suite, and therefore
every time a commit is pushed to github main branch
"""

import shutil
import os


try:
    shutil.rmtree('layer_utils/python/utils')
except FileNotFoundError:
    pass


#copy files in to directory
src_dir = "./utils"
dest_dir = "./layer_utils/python/utils"
files = os.listdir(src_dir)
shutil.copytree(src_dir, dest_dir)