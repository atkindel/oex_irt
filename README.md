## oex_irt
Tools for producing IRT-ready datasets from OpenEdX tracking data.

### Usage

#### Acquiring data
- **make generate-raws**: Build raw data files on remote host.
- **make fetch-raws**: Pull raw data files to local directory.

#### Transforming data
- **make parse**: Calculate IRT matrices from raw data files.

#### Cleaning up files
- **make local-clean**: Remove raw files from local directory.
- **make remote-clean**: Remove raw files from remote host.
