import os
import subprocess
import yaml

# 确保Imagine System路径中所有的中间目录都存在
thirdparty_dir_path = 'thirdparty/'
os.makedirs(os.path.dirname(thirdparty_dir_path), exist_ok = True)

system_dir_path = os.path.join(thirdparty_dir_path, 'Imagine_System')

# 判断Imagine System目录是否存在
if os.path.isdir(system_dir_path):
    # Imagine System存在直接退出
    print("[MAKE INIT]: Imagine_System exists.")
else:
    #Imagine System不存在创建
    print("[MAKE INIT]: Imagine_System NOT exists, starting create...")
    submodule_command = ["git", "submodule", "add", "-f", "https://github.com/ImagineJHY/Imagine_System.git"]
    process = subprocess.Popen(submodule_command, cwd = thirdparty_dir_path)
    process.wait()