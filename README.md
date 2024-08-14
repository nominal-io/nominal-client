# ⬖ Nominal
Python client for Nominal test data, storage, &amp; compute

🚧 WIP - API and syntax subject to change

## Install

> pip3 install nominal

## Usage

### Set your API key

Retrieve your API key from /sandbox on your Nominal tenant

```py
import nominal as nm
nm.set_token(...)
```

### Upload a Dataset (4 lines)

```py
import nominal as nm
from nominal import Ingest, Dataset
dataset = Dataset(nm.data.penguins())
# dataset = Ingest().read_csv('../path/to/your/data.csv')
dataset.upload()
```

### Upload a Run (4 lines)

```py
import nominal as nm
from nominal import Run, Dataset
r = Run(datasets=[Dataset(nm.data.penguins())])
# r = Run(path='../path/to/your/data.csv')
r.upload()
```

### Update metadata of an existing Run (4 lines)

```py
from nominal import Run
r = Run(rid = 'ri.scout.gov-staging.run.ce205f7e-9ef1-4a8b-92ae-11edc77441c6')
r.title = 'my_new_run_title'
r.update()
```

### Compare changes madde to a Run locally

```py
from nominal import Run
r = Run(rid = 'ri.scout.gov-staging.run.ce205f7e-9ef1-4a8b-92ae-11edc77441c6')
r.title = 'my_new_run_title'
r.diff()
```

### Apply a Check to a Run

TODO
