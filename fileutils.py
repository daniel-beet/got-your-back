# File and path utility functions

import pywintypes
import win32file
import win32con

def change_file_times(fname, mtime=None, atime=None, ctime=None):
  winmtime = None if mtime == None else pywintypes.Time(mtime)
  winatime = None if atime == None else pywintypes.Time(atime)
  winctime = None if ctime == None else pywintypes.Time(ctime)

  winfile = win32file.CreateFile(
    fname, win32con.GENERIC_WRITE,
    win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE | win32con.FILE_SHARE_DELETE,
    None, win32con.OPEN_EXISTING,
    win32con.FILE_ATTRIBUTE_NORMAL, None)

  win32file.SetFileTime(winfile, winctime, winatime, winmtime, True)

  winfile.close()
