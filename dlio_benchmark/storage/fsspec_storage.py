"""
   Copyright (c) 2022, UChicago Argonne, LLC
   All Rights Reserved

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
from abc import ABC, abstractmethod
from time import time
import importlib
import os

from dlio_benchmark.common.constants import MODULE_STORAGE
from dlio_benchmark.common.enumerations import FsspecPlugin
from dlio_benchmark.storage.storage_handler import DataStorage, Namespace
from dlio_benchmark.common.enumerations import NamespaceType, MetadataType
from dlio_profiler.logger import fn_interceptor as Profile

try:
    import fsspec
except ImportError:
    fsspec = None

dlp = Profile(MODULE_STORAGE)

supported_fsspec_plugins = {
    FsspecPlugin.LOCAL_FS: {
                'type': NamespaceType.HIERARCHICAL,
                'plugin_name': 'file',
                'prefix': 'file://',
                'external_module': None
             },
    FsspecPlugin.S3_FS: {
                'type': NamespaceType.FLAT,
                'plugin_name': 's3',
                'prefix': 's3://',
                'external_module': 's3fs'
             }
}

class FsspecStorage(DataStorage):
    """
    Storage API for creating local filesystem files.
    """

    @dlp.log_init
    def __init__(self, namespace, fsspec_plugin, fsspec_extra_params):
        if fsspec is None:
            raise ModuleNotFoundError(
                "Package 'fsspec' must be installed in order to use fsspec-based storage types"
            )

        if not fsspec_plugin in supported_fsspec_plugins:
            raise Exception(
                "Unsupported fsspec plugin name '{0}' specified".format(fsspec_plugin)
            )

        plugin = supported_fsspec_plugins[fsspec_plugin]
        if plugin['external_module'] is not None:
            try:
                importlib.import_module(plugin['external_module'])
            except ImportError:
                raise ModuleNotFoundError(
                    "Package '{0}' must be installed in order to use fsspec plugin {1}".format(plugin['external_module'], fsspec_plugin)
                )

        self.fsspec_plugin = fsspec_plugin
        self.prefix = plugin['prefix']
        self.fs = fsspec.filesystem(plugin['plugin_name'], **fsspec_extra_params)
        self.namespace = Namespace(namespace, plugin['type'])

    @dlp.log
    def get_uri(self, id=None):
        if id is None:
            return self.prefix + self.namespace.name
        else:
            return self.prefix + os.path.join(self.namespace.name, id)

    # Namespace APIs
    @dlp.log
    def create_namespace(self, exist_ok=False):
        self.fs.makedirs(self.get_uri(), exist_ok=exist_ok)
        return True

    @dlp.log
    def get_namespace(self):
        return self.namespace.name

    # Metadata APIs
    @dlp.log
    def create_node(self, id, exist_ok=False):
        self.fs.makedirs(self.get_uri(id), exist_ok=exist_ok)
        return True

    @dlp.log
    def get_node(self, id=None):
        path = self.get_uri(id)
        if self.fs.exists(path):
            if self.fs.isdir(path):
                return MetadataType.DIRECTORY
            else:
                return MetadataType.FILE
        else:
            return None

# For local files, the output list contains relative pathnames if use_pattern is
# False, but absolute pathnames if use_pattern is True.  This is because 
# os.listdir() returns relative pathnames, and os.glob() returns absolute pathnames.
# s3fs always returns absolute pathnames, so we have to emulate the local file
# behavior here when use_pattern is False.  When use_pattern is True, fsspec file
# and s3fs backends don't include the path prefix, but that is needed when the file
# paths are used, so add that back here.

    @dlp.log
    def walk_node(self, id, use_pattern=False):
        if not use_pattern:
            dlist = self.fs.listdir(self.get_uri(id), detail=False)

            if self.fsspec_plugin == FsspecPlugin.S3_FS:
                prefix = os.path.join(self.namespace.name, id) + '/'
                for i in range(0, len(dlist)):
                    if dlist[i].startswith(prefix):
                        dlist[i] = dlist[i][len(prefix)]
        else:
            dlist = self.fs.glob(self.get_uri(id), detail=False)

            for i in range(0, len(dlist)):
                dlist[i] = self.prefix + dlist[i]

        return dlist

    @dlp.log
    def delete_node(self, id):
        self.fs.rm(self.get_uri(id), recursive=True)
        return True

    # TODO Handle partial read and writes
    @dlp.log
    def put_data(self, id, data, offset=None, length=None):
        with self.fs.open(self.get_uri(id), mode="w") as fd:
            fd.write(data)

    @dlp.log
    def get_data(self, id, data, offset=None, length=None):
        with self.fs.open(self.get_uri(id), mode="r") as fd:
            data = fd.read()
        return data

    @dlp.log
    def get_flobj(self, uri_path, mode="rb"):
        return self.fs.open(uri_path, mode=mode)
    
    def get_basename(self, id):
        return os.path.basename(id)
