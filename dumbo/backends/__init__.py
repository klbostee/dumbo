# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


def get_backend(opts):
    """ Returns the first backend that matches with the given opts """
    for backend in backends:
        if backend.matches(opts):
            return backend

def create_iteration(opts):
    """ Creates iteration object using the first backend that matches """
    return get_backend(opts).create_iteration(opts)

def create_filesystem(opts):
    """ Creates filesystem object using the first backend that matches """
    return get_backend(opts).create_filesystem(opts)


#################################################################
## [!] The functions being above the import below is important ##
##     due to circular dependencies.                           ##
#################################################################

from dumbo.backends import streaming, unix

backends = [
        streaming.StreamingBackend(),
        unix.UnixBackend()
    ]