import ctypes
import os
import winreg


# YOU MUST RUN THIS SCRIPT AS LOCAL ADMIN to write to the registry!
# YOU MUST HAVE PYTHON IN THE PATH SO THAT CMD RECOGNIZES IT
# There are no external dependencies, no need to set venv for these steps:
#
# 1. right-click Command Prompt | Run as administrator
# 2. python setup.py


def print_key_info(key_base, key_path):
    try:
        # Open the registry key
        key = winreg.OpenKey(key_base, key_path, 0, winreg.KEY_READ)

        # Get key information
        num_subkeys, num_values, last_modified = winreg.QueryInfoKey(key)

        print(f"Key: {key_path}")
        print(f"Number of subkeys: {num_subkeys}")
        print(f"Number of values: {num_values}")
        print(f"Last modified: {last_modified}")
        print("\nValues:")

        # Enumerate and print all values
        for i in range(num_values):
            name, data, type = winreg.EnumValue(key, i)
            print(f"  {name}: {data} (Type: {type})")

        print("\nSubkeys:")

        # Enumerate and print all subkeys
        for i in range(num_subkeys):
            subkey_name = winreg.EnumKey(key, i)
            print(f"  {subkey_name}")

        # Close the key
        winreg.CloseKey(key)

    except WindowsError as e:
        print(f"Error accessing registry key: {e}")


###############################################################################


def register_odbc_dll() -> None:
    hklm = winreg.ConnectRegistry(None, winreg.HKEY_LOCAL_MACHINE)
    key_name = r"SOFTWARE\ODBC\ODBCINST.INI\DBISAM 4 ODBC Driver"
    dbisam_key = winreg.CreateKey(hklm, key_name)

    print(f"Installing {key_name}")

    # The .dll is in the python lib's subfolder. Move it if you must.
    dll_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dbodbc.dll")

    winreg.SetValueEx(dbisam_key, "APILevel", 0, winreg.REG_SZ, "1")
    winreg.SetValueEx(dbisam_key, "ConnectFunctions", 0, winreg.REG_SZ, "YYY")
    winreg.SetValueEx(dbisam_key, "Driver", 0, winreg.REG_SZ, dll_path)
    winreg.SetValueEx(dbisam_key, "DriverODBCVer", 0, winreg.REG_SZ, "03.00")
    winreg.SetValueEx(dbisam_key, "FileExtns", 0, winreg.REG_SZ, "*.dat,*.idx,*.blb")
    winreg.SetValueEx(dbisam_key, "FileUsage", 0, winreg.REG_SZ, "1")
    winreg.SetValueEx(dbisam_key, "Setup", 0, winreg.REG_SZ, dll_path)
    winreg.SetValueEx(dbisam_key, "SQLLevel", 0, winreg.REG_SZ, "0")
    winreg.SetValueEx(dbisam_key, "UsageCount", 0, winreg.REG_DWORD, 1)

    print_key_info(hklm, key_name)


def register_du() -> None:
    hkcu = winreg.ConnectRegistry(None, winreg.HKEY_CURRENT_USER)
    key_name = r"SOFTWARE\Sysinternals\Du"
    du_key = winreg.CreateKey(hkcu, key_name)

    print(f"Installing {key_name}")

    winreg.SetValueEx(du_key, "EulaAccepted", 0, winreg.REG_DWORD, 1)
    print_key_info(hkcu, key_name)


def is_admin():
    """returns 1 if admin, 0 if not"""
    x = ctypes.windll.shell32.IsUserAnAdmin()
    return x > 0


def prepare():
    if is_admin():
        register_odbc_dll()
        register_du()
    else:
        print("YOU MUST RUN THIS SCRIPT AS ADMIN to write to the registry!")
        print("YOU MUST HAVE PYTHON IN THE PATH SO THAT CMD RECOGNIZES IT")
        print("1. right-click Command Prompt | Run as administrator")
        print("2. python setup.py")

        # no luck getting this scheme to work, just warn to use Admin...
        # ctypes.windll.shell32.ShellExecuteW(
        #     None, "runas", sys.executable, " ".join(sys.argv), None, 1
        # )
