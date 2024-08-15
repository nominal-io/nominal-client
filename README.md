# ⬖ Nominal
Python client for Nominal test data, storage, &amp; compute

🚧 WIP - API and syntax subject to change

## Install

> pip3 install nominal --upgrade

## Usage

### Setup

Retrieve your API key from [/sandbox](https://app.gov.nominal.io/sandbox) on your Nominal tenant

```py
import nominal as nm
nm.cloud.set_token(...)
```

Change base URL to staging for dev or a custom URL

```py
import nominal as nm
nm.cloud.set_base_url('STAGING')
nm.cloud.set_base_url('https://nominal.acme.co')
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

### Compare changes made to a Run locally

```py
r.title = 'my_new__new_run_title'
r.diff()
```

### Apply a Check to a Run

TODO
