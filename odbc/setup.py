import os
import winreg

name = "DBISAM4"

# YOU MUST RUN THIS SCRIPT AS LOCAL ADMIN to write to the registry!
# YOU MUST HAVE PYTHON IN THE PATH SO THAT CMD RECOGNIZES IT
# There are no external dependencies, no need to set venv for these steps:
#
# 1. right-click Command Prompt | Run as administrator
# 2. python setup.py


# Only the 64-bit dll is added. We keep the default key name
hklm = winreg.ConnectRegistry(None, winreg.HKEY_LOCAL_MACHINE)
key_name = r"SOFTWARE\ODBC\ODBCINST.INI\DBISAM 4 ODBC Driver"

# The .dll is in the python lib's subfolder. Move it if you must.
dll = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dbodbc.dll")


dbisam_key = winreg.CreateKey(hklm, key_name)

winreg.SetValueEx(dbisam_key, "APILevel", 0, winreg.REG_SZ, "1")
winreg.SetValueEx(dbisam_key, "ConnectFunctions", 0, winreg.REG_SZ, "YYY")
winreg.SetValueEx(dbisam_key, "Driver", 0, winreg.REG_SZ, dll)
winreg.SetValueEx(dbisam_key, "DriverODBCVer", 0, winreg.REG_SZ, "03.00")
winreg.SetValueEx(dbisam_key, "FileExtns", 0, winreg.REG_SZ, "*.dat,*.idx,*.blb")
winreg.SetValueEx(dbisam_key, "FileUsage", 0, winreg.REG_SZ, "1")
winreg.SetValueEx(dbisam_key, "Setup", 0, winreg.REG_SZ, dll)
winreg.SetValueEx(dbisam_key, "SQLLevel", 0, winreg.REG_SZ, "0")
winreg.SetValueEx(dbisam_key, "UsageCount", 0, winreg.REG_DWORD, 1)


key = winreg.OpenKey(hklm, key_name, 0, winreg.KEY_READ)


def enum_registry_values(key: winreg.HKEYType) -> None:
    """
    Enumerate and print all values in a given registry key.

    Args:
    key: An open registry key.
    """
    print(f"\n\n{winreg.QueryInfoKey(key)[0]}\n")

    count = 0
    while True:
        try:
            name, value, type = winreg.EnumValue(key, count)
            print(f"{name:>20} -- {value}")
            count += 1
        except WindowsError as err:
            if err.winerror == 259:  # No more data is available
                break
            else:
                print(f"Unexpected error: {err}")
                break


key = winreg.OpenKey(hklm, key_name, 0, winreg.KEY_READ)

try:
    enum_registry_values(key)
finally:
    winreg.CloseKey(key)
