#
# Copyright 2015 David A. Boyuka II
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

SUBDIRS = include src tools tests

# pkg-config files, to be installed in lib/
pkgconfigdir = $(libdir)/pkgconfig
nodist_pkgconfig_DATA = pique.pc pique-parallel.pc

# Git commit info file for reference, to be installed in bin/
GITCOMMIT_FILENAME = git-commit.sha1
git-commit.sha1:
	( cd $(srcdir) && git rev-parse HEAD >/dev/null 2>/dev/null ) && \
	( cd $(srcdir) && git rev-parse HEAD ) > $(builddir)/$(GITCOMMIT_FILENAME)

gitcommitdir = $(bindir)
gitcommit_DATA = git-commit.sha1
