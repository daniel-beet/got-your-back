# run uding: python -OO setup.py install

from distutils.core import setup
import py2exe
import sys
import os

sys.argv.append('py2exe')

setup(name='<Name>',
      version='0.21',
      description='GYB - Google Mail Backup',
      author='Daniel Beet',
      console = ['gyb.py'],
      data_files = [("",["client_secrets.json", "cacert.pem",])],
      zipfile = None,
      options = {'py2exe': 
              {'optimize': 2,
               'bundle_files': 1,
               "dll_excludes": ["MPR.dll",
                                "MSWSOCK.dll",
                                "api-ms-win-core-apiquery-l1-1-0.dll",
                                "api-ms-win-core-console-l1-1-0.dll",
                                "api-ms-win-core-crt-l1-1-0.dll",
                                "api-ms-win-core-crt-l2-1-0.dll",
                                "api-ms-win-core-debug-l1-1-1.dll",
                                "api-ms-win-core-delayload-l1-1-1.dll",
                                "api-ms-win-core-errorhandling-l1-1-1.dll",
                                "api-ms-win-core-file-l1-2-0.dll",
                                "api-ms-win-core-handle-l1-1-0.dll",
                                "api-ms-win-core-heap-l1-2-0.dll",
                                "api-ms-win-core-heap-obsolete-l1-1-0.dll",
                                "api-ms-win-core-interlocked-l1-2-0.dll",
                                "api-ms-win-core-io-l1-1-1.dll",
                                "api-ms-win-core-libraryloader-l1-1-1.dll",
                                "api-ms-win-core-localization-l1-2-0.dll",
                                "api-ms-win-core-memory-l1-1-1.dll",
                                "api-ms-win-core-processenvironment-l1-2-0.dll",
                                "api-ms-win-core-processthreads-l1-1-1.dll",
                                "api-ms-win-core-profile-l1-1-0.dll",
                                "api-ms-win-core-registry-l1-1-0.dll",
                                "api-ms-win-core-string-l1-1-0.dll",
                                "api-ms-win-core-string-obsolete-l1-1-0.dll",
                                "api-ms-win-core-synch-l1-2-0.dll",
                                "api-ms-win-core-sysinfo-l1-2-0.dll",
                                "api-ms-win-core-util-l1-1-0.dll",
                                "api-ms-win-security-base-l1-2-0.dll",]}
            })