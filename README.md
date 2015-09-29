PIQUE - Parallel Indexing and Query Unified Engine
==================================================

PIQUE is a software package demonstrating the generalized indexing and query
framework described in the paper, "The Hyperdyadic Index and Generalized Indexing
and Query with PIQUE" (http://dl.acm.org/citation.cfm?id=2791374). Its purpose
is to provide a platform for the research and development of new indexing and query
methods that fall under the PIQUE model, and to facilitate the deployment of these
methods to end users.

The key features of PIQUE are:
* Modular implementations of key steps in the indexing and query pipeline, including:
  * Quantization (binning)
  * RSet representation (representation of sets of record IDs)
  * Index encoding (e.g., interval, range, binary component, etc.)
  * Index I/O backend (POSIX, MPI)
  * Query processing strategy (set operations-based, bitmap conversion-based)
* Maximal core implementations of shared algorithms that mimimize the effort needed
  to develop new modular implementations.
* A set of tools for end users to build, query, and inspect indexes over data.

Dependencies
------------

PIQUE has the following _required_ dependencies:

* **C99** and **C++11** compliant compilers (GNU gcc/g++ recommended)
* **Boost 1.55** or greater (http://www.boost.org/) (older versions _may_ work, version
  1.55 is most tested)
  * Specifically, Boost **Base**, **Serialization** and **Timer** libraries are required
* **MPI 2.1**-compliant library or newer
* A **specially-modified fork of FastBit 1.3.9**
  * (this is not generally available at the moment; this dependency will be removed when possible)
* **libridcompress**, available here: <link to be added>

PIQUE has the following _optional_ dependencies:

* **HDF5 1.8.13** or greater (older versions _may_ work)
  * If this dependency is configured, PIQUE will support indexing variables stored in HDF5 files

In the future, it is planned to make some of the required dependencies optional (specifically:
FastBit, libridcompress, and possibly MPI).

Building PIQUE
--------------

Once all dependencies have been built, PIQUE may be configured, built, and installed using the following
commands:

```
./configure \
  --with-boost=<path to Boost install> \
  --with-ridcompress=<path to ridcompress build> \
  --with-fastbit=<path to FastBit install> \
  --with-hdf5=<path to HDF5>
  
make
make install
```

Paths to Boost and HDF5 may be replaced with "yes" to perform automatic path detection.
`--with-hdf5` is optional, as discussed above.

The PIQUE distribution includes self-tests, which may be built and run as follows:

```
make check
```

Contact
-------

If you have any questions, comments, bug reports, etc. regarding PIQUE, please contact
Nagiza Samatova at nagiza.samatova@gmail.com. Thanks!
